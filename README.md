# DinoDB

This is the stencil code for the DinoDB assignments for Brown University's Database Management Systems course (CSCI 1270).

## Development Environment Setup

Refer to the [dev-environment setup guide](https://docs.google.com/document/d/1jZ2kigxwrrWe1N2YjJyFiwM4tMqcr4FYjE-aliOvhrI/edit) for instructions on how to set up the development environment for this repo. This is important to ensure you have all the necesary dependencies to run everything (such as Go and Task). Going forward, you should do all development work from within this dev environment.

## How to run stuff

This repo has a [Taskfile](https://taskfile.dev/) that includes code for carrying out common tasks during development. If you've used Makefiles before in a previous course, you can think of Taskfiles as an easier-to-use Makefile replacement. You can run the `task` commands in any directory within this repo as long as you are within the course's dev environment.

To get an overview of all the Tasks available to run, run `task --list` or `task -l`. To execute a specific task, run `task {TASK-NAME}`. For more details on a specific task (including as arguments that can be passed), run `task --summary {TASK-NAME}`.

In general, our Taskfile is a wrapper around the [`go` command](https://pkg.go.dev/cmd/go) - you can inspect the `Taskfile.yaml` file manually (or run `task --summary ...`) for more details on what specific commands are run.
