# Contributing Guide

Thank you for considering a contribution to this project!  We welcome
bug reports, feature requests, and code contributions.  To ensure a
smooth process, please follow the guidelines below.

## Getting started

1. **Fork the repository** on GitHub and clone your fork locally.
2. Create a new branch for each change you intend to make.  Use a
   descriptive name, e.g., `fix‑csv‑path` or `feature‑support‑year‑2026`.
3. Install the project’s dependencies and pre‑commit hooks:

   ```bash
   pip install -r requirements.txt -r requirements-dev.txt
   pre-commit install
   ```

## Development workflow

1. **Discuss** your idea.  Open a new issue or comment on an existing
   one before beginning significant work.  This helps avoid
   duplicated effort and ensures that proposed changes align with
   project goals.
2. **Make changes** on your feature branch.  Keep commits small and
   focused.  Write clear commit messages that explain the why and
   what of your changes.
3. **Run the pre‑commit hooks** before pushing:

   ```bash
   pre-commit run --all-files
   ```

   This will format your code with Black, run Ruff for static
   analysis, and fix trailing whitespace and end‑of‑file newlines.
4. **Write or update documentation** as needed.  If your change
   modifies how the scripts are used, update `docs/USAGE.md` and
   include examples.
5. **Submit a pull request**.  Reference the relevant issue in the PR
   description and include any necessary context.  A GitHub Actions
   workflow will automatically run linting checks on your PR.

## Code style and testing

We use [Black](https://black.readthedocs.io/) for code formatting
and [Ruff](https://github.com/astral-sh/ruff) for linting.  The
pre‑commit configuration runs both tools automatically.  All code
should be type‑annotated where practical.  Please ensure that your
changes adhere to these standards before submitting a PR.

## Reporting issues

If you encounter a bug or have a feature request, please open an
issue using the appropriate template.  Provide as much detail as
possible, including steps to reproduce the problem and versions of
software used (Stata, Python, operating system).

## Code of Conduct

This project adheres to the [Contributor Covenant Code of
Conduct](CODE_OF_CONDUCT.md).  By participating, you agree to
uphold these guidelines.  Instances of abusive, harassing, or
otherwise unacceptable behavior may be reported to the maintainers.