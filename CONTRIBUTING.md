# Contributing to Celesta

:+1::tada: First off, thanks for taking the time to contribute! :tada::+1:

[Code of conduct](CODE_OF_CONDUCT.md)

Feel free to open an issue on GitHub or submit a PR — every contribution is welcomed!

If you are willing to add a major new feature or modify public APIs, the point to start is [Celesta Improvement Process](https://github.com/courseorchestra/cip).
There we discuss new major/compatibility breaking proposals before the implementation. In order to initiate such a discussion you might want to submit a new CIP.

---

## How to Build Celesta

Celesta is using [TestContainers](https://www.testcontainers.org/) for tests. If you want to build Celesta with test run, you will need Docker on your machine. Allow some time and network traffic to automatically download the Docker images with the Celesta-supported database engines.

If you want to build `celesta-documentation` module locally, you will need to install [Graphviz](https://www.graphviz.org/) and [syntrax](https://github.com/kevinpt/syntrax). They are required to generate UML and syntax diagrams in the documentation.

## Pull Request Check List

In order for a PR to be merged into the main branch, the following conditions should be met:

* CI build should pass. CI build includes test run, static analysis checks and documentation spell check.
If spell check fails on a correctly spelled word that is missing in the dictionary, `dict` file located in the root of the project should be updated.

* Every code change should be covered by automated tests (this is verified during the code review).

* Where applicable, the documentation should be updated to reflect the changes. Celesta User Guide in Asciidoctor format is located in `celesta-documentation` module.
All public APIs should have JavaDocs.

* In order to be correctly shown in change logs, the PR should be tagged (e.g. with`enhancement` or`bugfix` tag).