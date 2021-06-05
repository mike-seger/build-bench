plugins {
	`java-library-conventions`
}

description = "JUnit Jupiter (Aggregator)"

dependencies {
	api(platform(projects.junitBom))
	api(projects.junitJupiterApi)
	api(projects.junitJupiterParams)

	runtimeOnly(projects.junitJupiterEngine)
}
