/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright 2020-2022 The JReleaser authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jreleaser.sdk.nexus;

import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import feign.Feign;
import feign.FeignException;
import feign.Request;
import feign.Response;
import feign.RetryableException;
import feign.Util;
import feign.auth.BasicAuthRequestInterceptor;
import feign.codec.DecodeException;
import feign.codec.Decoder;
import feign.codec.ErrorDecoder;
import feign.form.FormData;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import org.apache.tika.Tika;
import org.apache.tika.mime.MediaType;
import org.jreleaser.model.JReleaserVersion;
import org.jreleaser.sdk.nexus.api.Data;
import org.jreleaser.sdk.nexus.api.NexusAPI;
import org.jreleaser.sdk.nexus.api.NexusAPIException;
import org.jreleaser.sdk.nexus.api.PromoteRequest;
import org.jreleaser.sdk.nexus.api.StagedRepository;
import org.jreleaser.sdk.nexus.api.StagingProfile;
import org.jreleaser.util.JReleaserLogger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;
import static org.jreleaser.util.StringUtils.requireNonBlank;

/**
 * @author Andres Almiray
 * @since 1.1.0
 */
public class Nexus {
    private final JReleaserLogger logger;
    private final NexusAPI api;
    private final boolean dryrun;
    private final Tika tika = new Tika();

    public Nexus(JReleaserLogger logger, String apiHost, String username, String password, boolean dryrun) {
        requireNonNull(logger, "'logger' must not be blank");
        requireNonBlank(apiHost, "'apiHost' must not be blank");
        requireNonBlank(username, "'username' must not be blank");
        requireNonBlank(password, "'password' must not be blank");

        this.logger = logger;
        this.dryrun = dryrun;
        this.api = Feign.builder()
            .encoder(new JacksonEncoder())
            .decoder(new ContentNegotiationDecoder())
            .requestInterceptor(new BasicAuthRequestInterceptor(username, password))
            .requestInterceptor(template -> {
                template.header("User-Agent", "JReleaser/" + JReleaserVersion.getPlainVersion());
            })
            .errorDecoder(new NexusErrorDecoder())
            .options(new Request.Options(20, TimeUnit.SECONDS, 60, TimeUnit.SECONDS, true))
            .target(NexusAPI.class, apiHost);

        this.logger.debug("Nexus dryrun set to {}", dryrun);
    }

    public String findStagingProfileId(String groupId) throws NexusException {
        logger.debug("lookup staging profile for groupId: {}", groupId);

        return wrap(() -> {
            Data<List<StagingProfile>> data = api.getStagingProfiles();
            if (null == data || null == data.getData() || data.getData().isEmpty()) {
                throw fail("Did not find a matching staging profile for " + groupId);
            }

            return data.getData().stream()
                .filter(profile -> groupId.startsWith(profile.getName()) &&
                    (groupId.length() == profile.getName().length() ||
                        groupId.charAt(profile.getName().length()) == '.'))
                .max(Comparator.comparingInt(profile -> profile.getName().length()))
                .map(StagingProfile::getId)
                .orElseThrow(() -> fail("Did not find a matching staging profile for " + groupId));
        });
    }

    public String createStagingRepository(String profileId, String groupId) throws NexusException {
        logger.debug("creating staging repository [groupId: {}, profileId: {}]", groupId, profileId);
        return wrap(() -> {
            Data<StagedRepository> data = api.startStagingRepository(
                new Data<>(PromoteRequest.ofDescription("Staging repository for " + groupId)),
                profileId);
            if (null == data || null == data.getData()) {
                throw fail("Could not create staging repository");
            }

            return data.getData().getStagedRepositoryId();
        });
    }

    public void dropStagingRepository(String profileId, String stagingRepositoryId, String groupId) throws NexusException {
        logger.debug("dropping staging repository [stagingRepositoryId: {}]", stagingRepositoryId);
        wrap(() -> api.dropStagingRepository(
            new Data<>(PromoteRequest.of(stagingRepositoryId, "Staging repository for " + groupId)),
            profileId));
    }

    public void releaseStagingRepository(String profileId, String stagingRepositoryId, String groupId) throws NexusException {
        logger.debug("releasing staging repository [stagingRepositoryId: {}]", stagingRepositoryId);
        wrap(() -> api.releaseStagingRepository(
            new Data<>(PromoteRequest.of(stagingRepositoryId, "Staging repository for " + groupId)),
            profileId));
    }

    public void closeStagingRepository(String profileId, String stagingRepositoryId, String groupId) throws NexusException {
        logger.debug("closing staging repository [stagingRepositoryId: {}]", stagingRepositoryId);
        wrap(() -> api.closeStagingRepository(
            new Data<>(PromoteRequest.of(stagingRepositoryId, "Staging repository for " + groupId)),
            profileId));
    }

    public void upload(String stagingRepositoryId, String path, Path file) throws NexusException {
        String filename = file.getFileName().toString();
        logger.info(" - uploading {} as {}/{}", filename, path, filename);

        wrap(() -> {
            FormData formData = null;
            try {
                formData = toFormData(file);
            } catch (IOException e) {
                logger.error(" x failed to upload {}", filename, e);
                throw fail("Error encoding file " + filename, e);
            }
            api.upload(stagingRepositoryId, path, null);
        });
    }

    private NexusException fail(String message) {
        return new NexusException(message);
    }

    private NexusException fail(String message, Exception e) {
        return new NexusException(message, e);
    }

    private void wrap(NexusOperation operation) throws NexusException {
        try {
            if (!dryrun) operation.execute();
        } catch (NexusAPIException e) {
            logger.trace(e);
            throw new NexusException("Nexus operation failed", e);
        } catch (NexusException e) {
            logger.trace(e);
            throw e;
        }
    }

    private <T> T wrap(Callable<T> callable) throws NexusException {
        try {
            if (!dryrun) {
                return callable.call();
            }
            return null;
        } catch (NexusException e) {
            logger.trace(e);
            throw e;
        } catch (Exception e) {
            logger.trace(e);
            throw new NexusException("Nexus operation failed", e);
        }
    }

    private FormData toFormData(Path path) throws IOException {
        return FormData.builder()
            .fileName(path.getFileName().toString())
            .contentType(MediaType.parse(tika.detect(path)).toString())
            .data(Files.readAllBytes(path))
            .build();
    }

    interface NexusOperation {
        void execute() throws NexusException;
    }

    static class ContentNegotiationDecoder implements Decoder {
        private final XmlDecoder xml = new XmlDecoder();
        private final JacksonDecoder json = new JacksonDecoder();

        @Override
        public Object decode(Response response, Type type) throws IOException, FeignException {
            try {
                return xml.decode(response, type);
            } catch (NotXml e) {
                return json.decode(response, type);
            }
        }
    }

    static class XmlDecoder implements Decoder {
        private final XmlMapper mapper;

        public XmlDecoder() {
            this(new XmlMapper());
        }

        public XmlDecoder(XmlMapper mapper) {
            this.mapper = mapper;
        }

        @Override
        public Object decode(Response response, Type type) throws IOException {
            if (response.body() == null) {
                throw new NotXml();
            }

            Collection<String> value = response.headers().get("Content-Type");
            if (null == value || value.size() != 1) {
                throw new NotXml();
            }

            String contentType = value.iterator().next();
            if (!contentType.contains("application/xml")) {
                throw new NotXml();
            }

            Reader reader = response.body().asReader(Util.UTF_8);
            if (!reader.markSupported()) {
                reader = new BufferedReader(reader, 1);
            }
            try {
                // Read the first byte to see if we have any data
                reader.mark(1);
                if (reader.read() == -1) {
                    return null; // Eagerly returning null avoids "No content to map due to end-of-input"
                }
                reader.reset();
                return mapper.readValue(reader, mapper.constructType(type));
            } catch (RuntimeJsonMappingException e) {
                if (e.getCause() != null && e.getCause() instanceof IOException) {
                    throw (IOException) e.getCause();
                }
                throw e;
            }
        }
    }

    static class NotXml extends IOException {

    }

    static class NexusErrorDecoder implements ErrorDecoder {
        private final ErrorDecoder defaultErrorDecoder = new Default();

        @Override
        public Exception decode(String methodKey, Response response) {
            Exception exception = defaultErrorDecoder.decode(methodKey, response);

            if (exception instanceof RetryableException) {
                return exception;
            }

            if (response.status() >= 500) {
                return new RetryableException(
                    response.status(),
                    response.reason(),
                    response.request().httpMethod(),
                    null,
                    response.request());
            }

            return new NexusAPIException(response.status(), response.reason(), response.headers());
        }
    }
}
