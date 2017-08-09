
class Run55(object):

    jars = (
        "/ecrfiles/lib/EpisodeConstruction-v5.4.005b1014-20170510.jar:",
        "/ecrfiles/lib/external/javax.mail.jar:",
        "/ecrfiles/lib/external/log4j-1.2.17.jar:",
        "/ecrfiles/lib/external/mysql-connector-java-5.1.11-bin.jar:",
        "/ecrfiles/lib/external/opencsv-2.3.jar:",
        "/ecrfiles/lib/external/poi-3.10.1-20140818.jar:",
        "/ecrfiles/lib/external/poi-excelant-3.10.1-20140818.jar:",
        "/ecrfiles/lib/external/poi-ooxml-3.10.1-20140818.jar:",
        "/ecrfiles/lib/external/poi-ooxml-schemas-3.10.1-20140818.jar:",
        "/ecrfiles/lib/external/poi-scratchpad-3.10.1-20140818.jar:",
        "/ecrfiles/lib/external/stax-api-1.0.1.jar:",
        "/ecrfiles/lib/external/xmlbeans-2.6.0.jar:",
        "/ecrfiles/lib/external/dom4j-1.6.1.jar:",
        "/ecrfiles/lib/external/zip4j_1.3.2.jar:",
        "/ecrfiles/lib/external/jpa/hibernate-entitymanager-4.3.8.Final.jar:",
        "/ecrfiles/lib/external/jpa-metamodel-generator/hibernate-jpamodelgen-4.3.8.Final.jar:",
        "/ecrfiles/lib/external/required/antlr-2.7.7.jar:",
        "/ecrfiles/lib/external/required/dom4j-1.6.1.jar:",
        "/ecrfiles/lib/external/required/hibernate-commons-annotations-4.0.5.Final.jar:",
        "/ecrfiles/lib/external/required/hibernate-core-4.3.8.Final.jar:",
        "/ecrfiles/lib/external/required/hibernate-entitymanager-4.3.8.Final.jar:",
        "/ecrfiles/lib/external/required/hibernate-hql-parser-1.1.0.Final.jar:",
        "/ecrfiles/lib/external/required/hibernate-jpa-2.1-api-1.0.0.Final.jar:",
        "/ecrfiles/lib/external/required/hibernate-ogm-core-4.1.1.Final.jar:",
        "/ecrfiles/lib/external/required/jandex-1.1.0.Final.jar:",
        "/ecrfiles/lib/external/required/javassist-3.18.1-GA.jar:",
        "/ecrfiles/lib/external/required/jboss-logging-3.1.3.GA.jar:",
        "/ecrfiles/lib/external/required/jboss-logging-annotations-1.2.0.Beta1.jar:",
        "/ecrfiles/lib/external/required/jboss-transaction-api_1.2_spec-1.0.0.Final.jar:",
        "/ecrfiles/lib/external/required/stringtemplate-3.2.1.jar:",
        "/ecrfiles/lib/external/required/xml-apis-1.0.b2.jar:",
        "/ecrfiles/lib/external/c3p0/c3p0-0.9.2.1.jar:",
        "/ecrfiles/lib/external/c3p0/hibernate-c3p0-4.3.8.Final.jar:",
        "/ecrfiles/lib/external/c3p0/mchange-commons-java-0.2.3.4.jar:",
        "/ecrfiles/lib/external/com.fasterxml.jackson.annotations.jar:",
        "/ecrfiles/lib/external/com.fasterxml.jackson.core.jar:",
        "/ecrfiles/lib/external/com.fasterxml.jackson.databind.jar:",
        "/ecrfiles/lib/external/hibernate-ogm-mongodb-4.1.1.Final.jar:",
        "/ecrfiles/lib/external/jackson-core-asl.jar:",
        "/ecrfiles/lib/external/mongo-java-driver-2.12.4.jar:"
    )

    @staticmethod
    def cpath():
        return ''.join(Run55.jars)
