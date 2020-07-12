package org.apache.atlas.utils;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Properties;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class KafkaUtilsTest {

    @Test
    public void testSetKafkaJAASPropertiesForAllProperValues() {
        Properties properties = new Properties();
        Configuration configuration = new PropertiesConfiguration();

        final String loginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
        final String loginModuleControlFlag = "required";
        final String optionUseKeyTab = "false";
        final String optionStoreKey = "true";
        final String optionServiceName = "kafka";

        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleName",loginModuleName);
        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleControlFlag", loginModuleControlFlag);
        configuration.setProperty("atlas.jaas.KafkaClient.option.useKeyTab", optionUseKeyTab);
        configuration.setProperty("atlas.jaas.KafkaClient.option.storeKey", optionStoreKey);
        configuration.setProperty("atlas.jaas.KafkaClient.option.serviceName",optionServiceName);

        KafkaUtils.setKafkaJAASProperties(configuration, properties);
        String newPropertyValue = properties.getProperty(KafkaUtils.KAFKA_SASL_JAAS_CONFIG_PROPERTY);

        assertTrue(newPropertyValue.contains(loginModuleName), "loginModuleName not present in new property");
        assertTrue(newPropertyValue.contains(loginModuleControlFlag),"loginModuleControlFlag not present in new property");
        assertTrue(newPropertyValue.contains("useKeyTab=" + optionUseKeyTab), "useKeyTab not present in new property or value doesn't match");
        assertTrue(newPropertyValue.contains("storeKey="+ optionStoreKey), "storeKey not present in new property or value doesn't match");
        assertTrue(newPropertyValue.contains("serviceName=" + optionServiceName), "serviceName not present in new property or value doesn't match");

    }

    @Test
    public void testSetKafkaJAASPropertiesForMissingControlFlag() {
        Properties properties = new Properties();
        Configuration configuration = new PropertiesConfiguration();

        final String loginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
        final String loginModuleControlFlag = "required";
        final String optionUseKeyTab = "false";
        final String optionStoreKey = "true";
        final String optionServiceName = "kafka";

        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleName",loginModuleName);
        configuration.setProperty("atlas.jaas.KafkaClient.option.useKeyTab", optionUseKeyTab);
        configuration.setProperty("atlas.jaas.KafkaClient.option.storeKey", optionStoreKey);
        configuration.setProperty("atlas.jaas.KafkaClient.option.serviceName",optionServiceName);

        KafkaUtils.setKafkaJAASProperties(configuration, properties);
        String newPropertyValue = properties.getProperty(KafkaUtils.KAFKA_SASL_JAAS_CONFIG_PROPERTY);

        assertTrue(newPropertyValue.contains(loginModuleName), "loginModuleName not present in new property");
        assertTrue(newPropertyValue.contains(loginModuleControlFlag),"loginModuleControlFlag not present in new property");
        assertTrue(newPropertyValue.contains("useKeyTab=" + optionUseKeyTab), "useKeyTab not present in new property or value doesn't match");
        assertTrue(newPropertyValue.contains("storeKey="+ optionStoreKey), "storeKey not present in new property or value doesn't match");
        assertTrue(newPropertyValue.contains("serviceName=" + optionServiceName), "serviceName not present in new property or value doesn't match");

    }

    @Test
    public void testSetKafkaJAASPropertiesForMissingLoginModuleName() {
        Properties properties = new Properties();
        Configuration configuration = new PropertiesConfiguration();

        final String loginModuleControlFlag = "required";
        final String optionUseKeyTab = "false";
        final String optionStoreKey = "true";
        final String optionServiceName = "kafka";

        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleControlFlag", loginModuleControlFlag);
        configuration.setProperty("atlas.jaas.KafkaClient.option.useKeyTab", optionUseKeyTab);
        configuration.setProperty("atlas.jaas.KafkaClient.option.storeKey", optionStoreKey);
        configuration.setProperty("atlas.jaas.KafkaClient.option.serviceName",optionServiceName);

        KafkaUtils.setKafkaJAASProperties(configuration, properties);
        String newPropertyValue = properties.getProperty(KafkaUtils.KAFKA_SASL_JAAS_CONFIG_PROPERTY);

        assertNull(newPropertyValue);

    }

    @Test
    public void testSetKafkaJAASPropertiesWithSpecialCharacters() {
        Properties properties = new Properties();
        Configuration configuration = new PropertiesConfiguration();

        final String loginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
        final String loginModuleControlFlag = "required";
        final String optionKeyTabPath = "/path/to/file.keytab";
        final String optionPrincipal = "test/_HOST@EXAMPLE.COM";

        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleName",loginModuleName);
        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleControlFlag", loginModuleControlFlag);
        configuration.setProperty("atlas.jaas.KafkaClient.option.keyTabPath", optionKeyTabPath);
        configuration.setProperty("atlas.jaas.KafkaClient.option.principal", optionPrincipal);

        try {
            KafkaUtils.setKafkaJAASProperties(configuration, properties);
            String newPropertyValue = properties.getProperty(KafkaUtils.KAFKA_SASL_JAAS_CONFIG_PROPERTY);
            String updatedPrincipalValue = org.apache.hadoop.security.SecurityUtil.getServerPrincipal(optionPrincipal, (String) null);

            assertTrue(newPropertyValue.contains(loginModuleName), "loginModuleName not present in new property");
            assertTrue(newPropertyValue.contains(loginModuleControlFlag),"loginModuleControlFlag not present in new property");
            assertTrue(newPropertyValue.contains("keyTabPath=\"" + optionKeyTabPath + "\""));
            assertTrue(newPropertyValue.contains("principal=\""+ updatedPrincipalValue + "\""));

        } catch (IOException e) {
            fail("Failed while getting updated principal value with exception : " + e.getMessage());
        }

    }

//    @Test
//    public void testSetKafkaJAASPropertiesForTicketBasedLoginConfig() {
//        Properties properties = new Properties();
//        Configuration configuration = new PropertiesConfiguration();
//
//        final String loginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
//        final String loginModuleControlFlag = "required";
//        final String optionUseKeyTab = "false";
//        final String optionStoreKey = "true";
//        final String optionServiceName = "kafka";
//
//        configuration.setProperty("atlas.jaas.ticketBased-KafkaClient.loginModuleName",loginModuleName);
//        configuration.setProperty("atlas.jaas.ticketBased-KafkaClient.loginModuleControlFlag", loginModuleControlFlag);
//        configuration.setProperty("atlas.jaas.ticketBased-KafkaClient.option.useKeyTab", optionUseKeyTab);
//        configuration.setProperty("atlas.jaas.ticketBased-KafkaClient.option.storeKey", optionStoreKey);
//        configuration.setProperty("atlas.jaas.ticketBased-KafkaClient.option.serviceName",optionServiceName);
//
//        KafkaUtils kafkaUtils = new KafkaUtils(configuration);
//        KafkaUtils spyKafkaUtils = Mockito.spy(kafkaUtils);
//        when(spyKafkaUtils.isLoginKeytabBased()).thenReturn(false);
//        when(spyKafkaUtils.isLoginTicketBased()).thenReturn(true);
//        spyKafkaUtils.setKafkaJAASProperties(configuration, properties);
//        String newPropertyValue = properties.getProperty(KafkaUtils.KAFKA_SASL_JAAS_CONFIG_PROPERTY);
//
//        assertTrue(newPropertyValue.contains(loginModuleName), "loginModuleName not present in new property");
//        assertTrue(newPropertyValue.contains(loginModuleControlFlag),"loginModuleControlFlag not present in new property");
//        assertTrue(newPropertyValue.contains("useKeyTab=" + optionUseKeyTab), "useKeyTab not present in new property or value doesn't match");
//        assertTrue(newPropertyValue.contains("storeKey="+ optionStoreKey), "storeKey not present in new property or value doesn't match");
//        assertTrue(newPropertyValue.contains("serviceName=" + optionServiceName), "serviceName not present in new property or value doesn't match");
//    }
//
//    @Test
//    public void testSetKafkaJAASPropertiesForTicketBasedLoginFallback() {
//        Properties properties = new Properties();
//        Configuration configuration = new PropertiesConfiguration();
//
//        final String loginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
//        final String loginModuleControlFlag = "required";
//        final String optionUseKeyTab = "false";
//        final String optionStoreKey = "true";
//        final String optionServiceName = "kafka";
//
//        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleName",loginModuleName);
//        configuration.setProperty("atlas.jaas.KafkaClient.loginModuleControlFlag", loginModuleControlFlag);
//        configuration.setProperty("atlas.jaas.KafkaClient.option.useKeyTab", optionUseKeyTab);
//        configuration.setProperty("atlas.jaas.KafkaClient.option.storeKey", optionStoreKey);
//        configuration.setProperty("atlas.jaas.KafkaClient.option.serviceName",optionServiceName);
//
//        KafkaUtils kafkaUtils = new KafkaUtils(configuration);
//        KafkaUtils spyKafkaUtils = Mockito.spy(kafkaUtils);
//        when(spyKafkaUtils.isLoginKeytabBased()).thenReturn(false);
//        when(spyKafkaUtils.isLoginTicketBased()).thenReturn(true);
//        spyKafkaUtils.setKafkaJAASProperties(configuration, properties);
//        String newPropertyValue = properties.getProperty(KafkaUtils.KAFKA_SASL_JAAS_CONFIG_PROPERTY);
//
//        assertTrue(newPropertyValue.contains(loginModuleName), "loginModuleName not present in new property");
//        assertTrue(newPropertyValue.contains(loginModuleControlFlag),"loginModuleControlFlag not present in new property");
//        assertTrue(newPropertyValue.contains("useKeyTab=" + optionUseKeyTab), "useKeyTab not present in new property or value doesn't match");
//        assertTrue(newPropertyValue.contains("storeKey="+ optionStoreKey), "storeKey not present in new property or value doesn't match");
//        assertTrue(newPropertyValue.contains("serviceName=" + optionServiceName), "serviceName not present in new property or value doesn't match");
//    }
}
