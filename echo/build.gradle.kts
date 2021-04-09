plugins { kotlin("multiplatform") version Versions.Kotlin }

kotlin {
  jvm {
    compilations.all { kotlinOptions.jvmTarget = "1.8" }
    testRuns["test"].executionTask.configure { useJUnit() }
    withJava()
  }

  targets.all { compilations.all { kotlinOptions.allWarningsAsErrors = true } }

  sourceSets {
    val commonMain by getting {
      dependencies {
        implementation(kotlin("stdlib-common"))
        implementation(Deps.Kotlinx.CoroutinesCore)
        implementation(Deps.Kotlinx.ImmutableCollections)
      }
    }
    val commonTest by getting {
      dependencies {
        implementation(kotlin("test-common"))
        implementation(kotlin("test-annotations-common"))
      }
    }
    val jvmMain by getting
    val jvmTest by getting {
      dependencies {
        implementation(kotlin("test-junit"))
        implementation(Deps.Kotlinx.CoroutinesTest)
      }
    }
    all {
      languageSettings.enableLanguageFeature("InlineClasses")
      languageSettings.useExperimentalAnnotation("kotlin.RequiresOptIn")
      languageSettings.useExperimentalAnnotation("kotlin.ExperimentalUnsignedTypes")
      languageSettings.useExperimentalAnnotation("kotlinx.coroutines.ExperimentalCoroutinesApi")
      languageSettings.useExperimentalAnnotation("kotlinx.coroutines.FlowPreview")
    }
  }
}
