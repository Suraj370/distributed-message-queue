plugins {
	java
	id("org.springframework.boot") version "4.1.1"
	id("io.spring.dependency-management") version "1.1.7"
	id("com.diffplug.spotless") version "8.10.2"
	id("me.champeau.jmh") version "0.7.3"
}

group = "com.surajpanda"
version = "0.0.1-SNAPSHOT"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(25)
	}
}

repositories {
	mavenCentral()
}

// Testcontainers-based Docker integration tests live in their own source set, separate from the
// fast unit/in-process-integration suite in src/test. They need a running Docker daemon and are
// meaningfully slower (building an image, starting real containers), so they run as their own
// Gradle task rather than folding into the default `test` task - `check` still depends on both.
sourceSets {
	create("dockerTest") {
		java.srcDir("src/dockerTest/java")
		resources.srcDir("src/dockerTest/resources")
	}
}

configurations.named("dockerTestImplementation") {
	extendsFrom(configurations.getByName("testImplementation"))
}
configurations.named("dockerTestRuntimeOnly") {
	extendsFrom(configurations.getByName("testRuntimeOnly"))
}

dependencies {
	implementation("org.springframework.boot:spring-boot-starter-actuator")
	implementation("org.springframework.boot:spring-boot-starter-validation")
	implementation("org.springframework.boot:spring-boot-starter-webmvc")
	compileOnly("org.projectlombok:lombok")
	developmentOnly("org.springframework.boot:spring-boot-devtools")
	annotationProcessor("org.projectlombok:lombok")
	testImplementation("org.springframework.boot:spring-boot-starter-actuator-test")
	testImplementation("org.springframework.boot:spring-boot-starter-validation-test")
	testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")
	testCompileOnly("org.projectlombok:lombok")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
	testAnnotationProcessor("org.projectlombok:lombok")

	"dockerTestImplementation"(platform("org.testcontainers:testcontainers-bom:1.20.4"))
	"dockerTestImplementation"("org.testcontainers:testcontainers")
	"dockerTestImplementation"("org.testcontainers:junit-jupiter")
}

tasks.withType<Test> {
	useJUnitPlatform()
}

val dockerTest = tasks.register<Test>("dockerTest") {
	description = "Runs Testcontainers-based multi-broker Docker integration tests (requires Docker)."
	group = "verification"
	testClassesDirs = sourceSets["dockerTest"].output.classesDirs
	classpath = sourceSets["dockerTest"].runtimeClasspath
	useJUnitPlatform()
	shouldRunAfter(tasks.named("test"))
	// Real container startup dominates runtime, not CPU - parallel forks would only add Docker
	// daemon contention, and interleaved container logs are hard enough to read as it is.
	maxParallelForks = 1
}

tasks.named("check") {
	dependsOn(dockerTest)
}

// JMH benchmarks live in their own source set (src/jmh/java), wired up automatically by the
// me.champeau.jmh plugin with main's classes on its compile classpath. They are opt-in via
// `./gradlew jmh` - deliberately not part of `check`/`build`, since they're slow, noisy under
// shared-CI-machine contention, and answer a different question ("how fast") than correctness.
//
// `includes` is the plugin's own supported filter - it is passed straight through to JMH, which
// matches each pattern as a regex `find()` against every benchmark's fully-qualified name (e.g.
// "com.surajpanda.dmq.benchmarks.PartitionAppendBenchmark.appendMessage"). `-PjmhInclude=<regex>`
// sets it for a single run; omitting the property keeps the default ".*" (every benchmark), so
// `./gradlew jmh` on its own still runs the full suite.
jmh {
	jmhVersion.set("1.37")
	includes.set(listOf((project.findProperty("jmhInclude") as String?) ?: ".*"))
}

spotless {
	java {
		target("src/*/java/**/*.java")
		googleJavaFormat()
		removeUnusedImports()
		trimTrailingWhitespace()
		endWithNewline()
	}
}

tasks.named("compileJava") {
	dependsOn("spotlessApply")
}
