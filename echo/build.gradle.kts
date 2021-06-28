plugins {
  kotlin(Plugins.KotlinMultiplatform)
  id(Plugins.KotlinBinaryCompatibility) version Versions.KotlinBinaryCompatibility
}

kotlin {
  jvm {
    compilations.all { kotlinOptions.jvmTarget = "1.8" }
    testRuns["test"].executionTask.configure { useJUnit() }
    withJava()
  }

  js(IR) { browser() }

  targets.all { compilations.all { kotlinOptions.allWarningsAsErrors = false } }

  sourceSets {
    val commonMain by getting {
      dependencies {
        implementation(kotlin("stdlib-common"))
        implementation(project(":echo-core"))
        api(Deps.Kotlinx.CoroutinesCore)
        implementation(Deps.Kotlinx.ImmutableCollections)
      }
    }
    val commonTest by getting {
      dependencies {
        implementation(kotlin("test-common"))
        implementation(kotlin("test-annotations-common"))

        implementation(Deps.CashApp.Turbine)
      }
    }
    val jvmMain by getting
    val jvmTest by getting {
      dependencies {
        implementation(kotlin("test-junit"))
        implementation(Deps.Kotlinx.CoroutinesTest)
      }
    }
    val jsMain by getting
    val jsTest by getting { dependencies { implementation(kotlin("test-js")) } }
    all {
      languageSettings.enableLanguageFeature("InlineClasses")
      languageSettings.useExperimentalAnnotation("kotlin.RequiresOptIn")
      languageSettings.useExperimentalAnnotation("kotlin.time.ExperimentalTime")
      languageSettings.useExperimentalAnnotation("kotlinx.coroutines.ExperimentalCoroutinesApi")
      languageSettings.useExperimentalAnnotation("kotlinx.coroutines.FlowPreview")
    }
  }
}
