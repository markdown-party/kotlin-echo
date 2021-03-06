plugins {
  kotlin(Plugins.KotlinMultiplatform)
  kotlin(Plugins.KotlinSerialization)
  id(Plugins.KotlinBinaryCompatibility) version Versions.KotlinBinaryCompatibility
}

kotlin {
  jvm {
    compilations.all { kotlinOptions.jvmTarget = "1.8" }
    testRuns["test"].executionTask.configure { useJUnit() }
    withJava()
  }

  js(IR) { browser() }

  targets.all { compilations.all { kotlinOptions.allWarningsAsErrors = true } }

  sourceSets {
    val commonMain by getting {
      dependencies {
        implementation(kotlin("stdlib-common"))
        api(Deps.Kotlinx.SerializationCore)
      }
    }
    val commonTest by getting {
      dependencies {
        implementation(kotlin("test-common"))
        implementation(kotlin("test-annotations-common"))
      }
    }
    val jvmMain by getting
    val jvmTest by getting { dependencies { implementation(kotlin("test-junit")) } }
    val jsMain by getting
    val jsTest by getting { dependencies { implementation(kotlin("test-js")) } }
    all {
      languageSettings.useExperimentalAnnotation("kotlin.ExperimentalUnsignedTypes")
      languageSettings.useExperimentalAnnotation("kotlin.RequiresOptIn")
      languageSettings.useExperimentalAnnotation("kotlin.time.ExperimentalTime")
    }
  }
}
