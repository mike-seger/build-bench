import com.gradle.enterprise.gradleplugin.internal.extension.BuildScanExtensionWithHiddenFeatures

pluginManagement {
	plugins {
		id("com.gradle.enterprise") version "3.6.1"
		id("com.gradle.enterprise.test-distribution") version "2.0.3"
		id("com.gradle.common-custom-user-data-gradle-plugin") version "1.3"
		id("net.nemerosa.versioning") version "2.14.0"
		id("com.github.ben-manes.versions") version "0.38.0"
		id("com.diffplug.spotless") version "5.12.4"
		id("org.ajoberstar.git-publish") version "3.0.0"
		kotlin("jvm") version "1.5.0"
		// Check if workaround in documentation.gradle.kts can be removed when upgrading
		id("org.asciidoctor.jvm.convert") version "3.3.2"
		id("org.asciidoctor.jvm.pdf") version "3.3.2"
		id("me.champeau.gradle.jmh") version "0.5.3"
		id("io.spring.nohttp") version "0.0.8"
		id("io.github.gradle-nexus.publish-plugin") version "1.1.0"
	}
}

plugins {
	id("com.gradle.enterprise")
	id("com.gradle.enterprise.test-distribution")
	id("com.gradle.common-custom-user-data-gradle-plugin")
}

val gradleEnterpriseServer = "https://ge.junit.org"
val isCiServer = System.getenv("CI") != null
val junitBuildCacheUsername: String? by extra
val junitBuildCachePassword: String? by extra

gradleEnterprise {
	buildScan {
		isCaptureTaskInputFiles = true
		isUploadInBackground = !isCiServer

		fun accessKeysAreMissingOrBlank() = System.getenv("GRADLE_ENTERPRISE_ACCESS_KEY").isNullOrBlank()

		if (gradle.startParameter.isBuildScan || (isCiServer && accessKeysAreMissingOrBlank())) {
			termsOfServiceUrl = "https://gradle.com/terms-of-service"
		} else {
			server = gradleEnterpriseServer
			publishAlways()
			this as BuildScanExtensionWithHiddenFeatures
			publishIfAuthenticated()
		}

		if (isCiServer) {
			publishAlways()
			termsOfServiceAgree = "yes"
		}

		obfuscation {
			if (isCiServer) {
				username { "github" }
			} else {
				hostname { null }
				ipAddresses { emptyList() }
			}
		}

		val enableTestDistribution = providers.gradleProperty("enableTestDistribution")
			.forUseAtConfigurationTime()
			.map(String::toBoolean)
			.getOrElse(false)
		if (enableTestDistribution) {
			tag("test-distribution")
		}
	}
}

buildCache {
	local {
		isEnabled = !isCiServer
	}
	remote<HttpBuildCache> {
		url = uri("$gradleEnterpriseServer/cache/")
		isPush = isCiServer && !junitBuildCacheUsername.isNullOrEmpty() && !junitBuildCachePassword.isNullOrEmpty()
		credentials {
			username = junitBuildCacheUsername?.ifEmpty { null }
			password = junitBuildCachePassword?.ifEmpty { null }
		}
	}
}

val javaVersion = JavaVersion.current()
require(javaVersion.isJava11Compatible) {
	"The JUnit 5 build requires Java 11 or higher. Currently executing with Java ${javaVersion.majorVersion}."
}

rootProject.name = "junit5"

include("documentation")
include("junit-jupiter")
include("junit-jupiter-api")
include("junit-jupiter-engine")
include("junit-jupiter-migrationsupport")
include("junit-jupiter-params")
include("junit-platform-commons")
include("junit-platform-console")
include("junit-platform-console-standalone")
include("junit-platform-engine")
include("junit-platform-jfr")
include("junit-platform-launcher")
include("junit-platform-reporting")
include("junit-platform-runner")
include("junit-platform-suite")
include("junit-platform-suite-api")
include("junit-platform-suite-commons")
include("junit-platform-suite-engine")
include("junit-platform-testkit")
include("junit-vintage-engine")
include("platform-tests")
include("platform-tooling-support-tests")
include("junit-bom")

// check that every subproject has a custom build file
// based on the project name
rootProject.children.forEach { project ->
	project.buildFileName = "${project.name}.gradle"
	if (!project.buildFile.isFile) {
		project.buildFileName = "${project.name}.gradle.kts"
	}
	require(project.buildFile.isFile) {
		"${project.buildFile} must exist"
	}
}

enableFeaturePreview("VERSION_CATALOGS")
enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")
