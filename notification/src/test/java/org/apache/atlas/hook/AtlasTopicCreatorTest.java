/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.hook;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.utils.KafkaUtils;
import org.apache.commons.configuration.Configuration;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyShort;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AtlasTopicCreatorTest {

    private final String ATLAS_HOOK_TOPIC     = AtlasConfiguration.NOTIFICATION_HOOK_TOPIC_NAME.getString();
    private final String ATLAS_ENTITIES_TOPIC = AtlasConfiguration.NOTIFICATION_ENTITIES_TOPIC_NAME.getString();

    @Test
    public void shouldNotCreateAtlasTopicIfNotConfiguredToDoSo() {

        Configuration configuration = mock(Configuration.class);
        when(configuration.getBoolean(AtlasTopicCreator.ATLAS_NOTIFICATION_CREATE_TOPICS_KEY, true)).
                thenReturn(false);
        when(configuration.getString("atlas.authentication.method.kerberos")).thenReturn("false");

        final KafkaUtils mockKafkaUtils = mock(KafkaUtils.class);

        AtlasTopicCreator atlasTopicCreator = new AtlasTopicCreator() {
            @Override
            KafkaUtils createKafkaUtils(Configuration atlasProperties) {
                return mockKafkaUtils;
            }
        };
        atlasTopicCreator.createAtlasTopic(configuration, ATLAS_HOOK_TOPIC);
        verify(mockKafkaUtils, never()).createTopics(any(), anyInt(), anyShort());
    }

    @Test
    public void shouldNotCreateTopicIfItAlreadyExists() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getBoolean(AtlasTopicCreator.ATLAS_NOTIFICATION_CREATE_TOPICS_KEY, true)).
                thenReturn(true);
        when(configuration.getString("atlas.authentication.method.kerberos")).thenReturn("false");

        final KafkaUtils mockKafkaUtils = mock(KafkaUtils.class);
        when(mockKafkaUtils.listAllTopics()).thenReturn(Collections.singletonList(ATLAS_HOOK_TOPIC));

        AtlasTopicCreator atlasTopicCreator = new AtlasTopicCreator() {
            @Override
            KafkaUtils createKafkaUtils(Configuration atlasProperties) {
                return mockKafkaUtils;
            }
        };

        atlasTopicCreator.createAtlasTopic(configuration, ATLAS_HOOK_TOPIC);

        verify(mockKafkaUtils, never()).createTopics(any(), anyInt(), anyShort());
    }

    @Test
    public void shouldCreateTopicIfItDoesNotExist() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getBoolean(AtlasTopicCreator.ATLAS_NOTIFICATION_CREATE_TOPICS_KEY, true)).
                thenReturn(true);
        when(configuration.getString("atlas.authentication.method.kerberos")).thenReturn("false");

        final KafkaUtils mockKafkaUtils = mock(KafkaUtils.class);
        when(mockKafkaUtils.listAllTopics()).thenReturn(Collections.emptyList());

        AtlasTopicCreator atlasTopicCreator = new AtlasTopicCreator() {
            @Override
            KafkaUtils createKafkaUtils(Configuration atlasProperties) {
                return mockKafkaUtils;
            }
        };

        atlasTopicCreator.createAtlasTopic(configuration, ATLAS_HOOK_TOPIC);

        verify(mockKafkaUtils).createTopics(eq(Collections.singletonList(ATLAS_HOOK_TOPIC)), anyInt(), anyShort());
    }

    @Test
    public void shouldNotFailIfExceptionOccursDuringCreatingTopic() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getBoolean(AtlasTopicCreator.ATLAS_NOTIFICATION_CREATE_TOPICS_KEY, true)).
                thenReturn(true);
        when(configuration.getString("atlas.authentication.method.kerberos")).thenReturn("false");

        final KafkaUtils mockKafkaUtils = mock(KafkaUtils.class);
        when(mockKafkaUtils.listAllTopics()).thenReturn(Collections.emptyList());
        doThrow(new RuntimeException("Simulating failure during creating topic"))
                .when(mockKafkaUtils).createTopics(any(), anyInt(), anyShort());

        AtlasTopicCreator atlasTopicCreator = new AtlasTopicCreator() {
            @Override
            KafkaUtils createKafkaUtils(Configuration atlasProperties) {
                return mockKafkaUtils;
            }
        };

        atlasTopicCreator.createAtlasTopic(configuration, ATLAS_HOOK_TOPIC);

        verify(mockKafkaUtils).createTopics(eq(Collections.singletonList(ATLAS_HOOK_TOPIC)), anyInt(), anyShort());
    }

    @Test
    public void shouldCreateMultipleTopics() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getBoolean(AtlasTopicCreator.ATLAS_NOTIFICATION_CREATE_TOPICS_KEY, true)).
                thenReturn(true);
        when(configuration.getString("atlas.authentication.method.kerberos")).thenReturn("false");

        final KafkaUtils mockKafkaUtils = mock(KafkaUtils.class);
        when(mockKafkaUtils.listAllTopics()).thenReturn(Collections.emptyList());

        AtlasTopicCreator atlasTopicCreator = new AtlasTopicCreator() {
            @Override
            KafkaUtils createKafkaUtils(Configuration atlasProperties) {
                return mockKafkaUtils;
            }
        };

        atlasTopicCreator.createAtlasTopic(configuration, ATLAS_HOOK_TOPIC, ATLAS_ENTITIES_TOPIC);

        verify(mockKafkaUtils).createTopics(eq(Arrays.asList(ATLAS_HOOK_TOPIC, ATLAS_ENTITIES_TOPIC)), anyInt(), anyShort());
    }

    /* TODO: this should be moved to KafkaUtils somehow, logic changed, all topics are sent once in a list to KafkaUtils.createTopics, this logic is handle in that
    @Test
    public void shouldCreateTopicEvenIfEarlierOneFails() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getBoolean(AtlasTopicCreator.ATLAS_NOTIFICATION_CREATE_TOPICS_KEY, true)).
                thenReturn(true);
        when(configuration.getString("atlas.authentication.method.kerberos")).thenReturn("false");
        final ZkUtils zookeeperUtils = mock(ZkUtils.class);

        final Map<String, Boolean> createdTopics = new HashMap<>();
        createdTopics.put(ATLAS_ENTITIES_TOPIC, false);

        AtlasTopicCreator atlasTopicCreator = new AtlasTopicCreator() {

            @Override
            protected boolean ifTopicExists(String topicName, ZkUtils zkUtils) {
                return false;
            }

            @Override
            protected ZkUtils createZkUtils(Configuration atlasProperties) {
                return zookeeperUtils;
            }

            @Override
            protected void createTopic(Configuration atlasProperties, String topicName, ZkUtils zkUtils) {
                if (topicName.equals(ATLAS_HOOK_TOPIC)) {
                    throw new RuntimeException("Simulating failure when creating ATLAS_HOOK topic");
                } else {
                    createdTopics.put(topicName, true);
                }
            }
        };
        atlasTopicCreator.createAtlasTopic(configuration, ATLAS_HOOK_TOPIC, ATLAS_ENTITIES_TOPIC);
        assertTrue(createdTopics.get(ATLAS_ENTITIES_TOPIC));
    }
    */

    @Test
    public void shouldCloseResources() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getBoolean(AtlasTopicCreator.ATLAS_NOTIFICATION_CREATE_TOPICS_KEY, true)).
                thenReturn(true);
        when(configuration.getString("atlas.authentication.method.kerberos")).thenReturn("false");

        final KafkaUtils mockKafkaUtils = mock(KafkaUtils.class);
        when(mockKafkaUtils.listAllTopics()).thenReturn(Collections.emptyList());

        AtlasTopicCreator atlasTopicCreator = new AtlasTopicCreator() {
            @Override
            KafkaUtils createKafkaUtils(Configuration atlasProperties) {
                return mockKafkaUtils;
            }
        };

        atlasTopicCreator.createAtlasTopic(configuration, ATLAS_HOOK_TOPIC, ATLAS_ENTITIES_TOPIC);

        verify(mockKafkaUtils).close();
    }

    @Test
    public void shouldNotProcessTopicCreationIfSecurityFails() {
        Configuration configuration = mock(Configuration.class);
        when(configuration.getBoolean(AtlasTopicCreator.ATLAS_NOTIFICATION_CREATE_TOPICS_KEY, true)).
                thenReturn(true);

        final KafkaUtils mockKafkaUtils = mock(KafkaUtils.class);
        when(mockKafkaUtils.listAllTopics()).thenReturn(Collections.emptyList());

        AtlasTopicCreator atlasTopicCreator = new AtlasTopicCreator() {
            @Override
            KafkaUtils createKafkaUtils(Configuration atlasProperties) {
                return mockKafkaUtils;
            }

            @Override
            boolean handleSecurity(Configuration atlasProperties) {
                return false;
            }
        };

        atlasTopicCreator.createAtlasTopic(configuration, ATLAS_HOOK_TOPIC, ATLAS_ENTITIES_TOPIC);

        verify(mockKafkaUtils, never()).createTopics(any(), anyInt(), anyShort());
    }
}
