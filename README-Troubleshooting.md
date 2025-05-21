# Apache Flink Troubleshooting Exercises

Exercises that accompany the troubleshooting training content in the documentation.

## Table of Contents

[**Set up your development environment**](#set-up-your-development-environment)

1. [Software requirements](#software-requirements)
1. [Download and build the flink-training project](#clone-and-build-the-flink-training-project)
1. [Import the flink-training project into your IDE](#import-the-flink-training-project-into-your-ide)

[**How to do the lab exercises**](#how-to-do-the-lab-exercises)

1. [Run and debug Flink programs in your IDE](#run-and-debug-flink-programs-in-your-ide)
1. [Exercises, tests, and solutions](#exercises-tests-and-solutions)

[**Lab exercises**](#lab-exercises)

## Set up your development environment

You will need to set up your environment in order to develop, debug, and execute solutions to 
the training exercises and examples.

### Software requirements

Flink supports Linux, OS X, and Windows as development environments for Flink programs and 
local execution. The following software is required for a Flink development setup and should 
be installed on your system:

- a JDK for Java 17 (a JRE is not sufficient; other versions of Java are currently not supported)
- an IDE for Java development with Gradle support
  - We recommend [IntelliJ](https://www.jetbrains.com/idea/), but [Eclipse](https://www.eclipse.org/downloads/) or 
    [Visual Studio Code](https://code.visualstudio.com/) (with the [Java extension pack](https://code.visualstudio.
    com/docs/java/java-tutorial)) can also be used so long as you stick to Java. 
  - The recent Eclipse comes with Java 21, make sure you configure the Gradle plugin to use Java 17.

> **:information_source: Note for Windows users:** The shell command examples provided in the training instructions are for UNIX systems.
> You may find it worthwhile to setup cygwin or WSL. For developing Flink jobs, Windows works reasonably well: you can run a Flink cluster on a single machine, submit jobs, run the webUI, and execute jobs in the IDE.

### Download and build the flink-training project

The `flink-training-troubleshooting.zip` archive contains exercises, tests, and reference solutions for 
the programming exercises.

Download the `flink-training-troubleshooting.zip` archive, unpack it, and build it:

```bash
wget -qO- http://blah | tar xvz -C /target/directory
cd /target/directory/flink-training-troubleshooting
./gradlew testSolutions shadowJar
```

If this is your first time building it, you will end up downloading all of the dependencies for this Flink training
project. This usually takes a few minutes, depending on the speed of your internet connection.

If all of the tests pass and the build is successful, you are off to a good start.

### Import the flink-training-troubleshooting project into your IDE

The project needs to be imported as a gradle project into your IDE.

Then you should be able to open [`TroubledStreamingJob`](troubleshooting/introduction/src/main/java/com/ververica/flink/training/exercises/TroubledStreamingJob.java) 
and run the main method.

> **:information_source: Note for Eclipse users:** Several Gradle projects in this repo 
> depend on the Gradle project `common`. In order for Eclipse to detect the Gradle project dependencies correctly:
> You likely need to run the following command:
> 
> `cd flink-training-troubleshooting; ./gradlew cleanEclipse cleanEclipseProject cleanEclipseClasspath eclipse`
> 
> Then, in the Gradle project that depends on `common`, set `Without test code` to `No` in the project dependence 
> setting. See the screenshot: 
> ![dependency-fix](images/project-dependency-fix-test-code.png)

## How to do the lab exercises

In the labs, you will implement Flink programs using various Flink APIs.

The following steps guide you through the process of using the provided data streams, 
implementing your Flink streaming program, and executing your program in your IDE.

We assume you have set up your development environment according to our
[setup guide](#set-up-your-development-environment).

### Run and debug Flink programs in your IDE

Flink programs can be executed and debugged from within an IDE. This significantly eases the 
development process and provides an experience similar to working on any other Java application.

To start a Flink program in your IDE, run its `main()` method. Under the hood, the execution 
environment will start a local Flink instance within the same process. Hence, it is also 
possible to put breakpoints in your code and debug it.

If you have an IDE with this `flink-training-troubleshooting` project imported, you can run 
(or debug) a streaming job by:

- opening the `com.ververica.flink.training.exercises.TroubledStreamingJob` class
- running (or debugging) the `main()` method of this class

### Exercises, tests, and solutions

Each of these exercises include:
- an `...Exercise` class with most of the necessary boilerplate code for getting started
- a JUnit Test class (`...Test`) with a few tests for your implementation
- a `...Solution` class with a complete solution.

If there are multiple exercises, the class name will be `...1Exercise`, `...2Exercise`, and so on.

You can run exercises, solutions, and tests with the `gradlew` command.

To run tests:

```bash
./gradlew test
./gradlew :troubleshooting:<subproject>:test
```

For exercises and solutions, we provide special tasks that can be listed with:

```bash
./gradlew printRunTasks
```

:point_down: Now you are ready to begin the lab exercises. :point_down:

## Lab exercises

TODO - see README-Bootcamp.md for format of entries.