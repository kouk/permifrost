# Contributing to permifrost

:+1::tada: Thank you for thinking about contributing! :tada::+1:

Contributing to open-source projects can be a really fun and validating
experience.  You've taken the first step already: reading this doc. Here we'll
give you some background about the project, and talk about ways in which you can
contribute.

You can even make changes to this guide! Don't like what you see? Think
something is missing? Open a merge-request with changes and start the
conversation.

#### Table of Contents

[Code of Conduct](#code-of-conduct)

[What should I know before I get
started?](#what-should-i-know-before-i-get-started)

[How Can I Contribute?](#how-can-i-contribute)
  * [Reporting Bugs](#reporting-bugs)
  * [Suggesting Enhancements](#suggesting-enhancements)
  * [Your First Code Contribution](#your-first-code-contribution)
  * [Pull Requests](#pull-requests)

## Code of Conduct

This project is governed by the [GitLab Code of
Conduct](https://about.gitlab.com/community/contribute/code-of-conduct/)


## What should I know before I get started?

### About Permifrost and Snowflake

Permifrost is a tool for managing
[permissions](https://docs.snowflake.com/en/user-guide/security-access-control-privileges.html)
in Snowflake.

Permifrost manages permissions for users, roles and the objects they use,
including databases, schemas, tables, and views. Generally this is done by
abstracting grants into read and/or write permissions. Permifrost also supports
usage grants for warehouses.

Permifrost also manages
[users](https://docs.snowflake.com/en/sql-reference/ddl-user-security.html#user-management)
and
[roles](https://docs.snowflake.com/en/sql-reference/ddl-user-security.html#role-management).

Roles are granted privileges to specific objects, and users are assigned roles
so that users have access to perform those privileges on those objects. Roles
can also be granted to other roles.

Another thing to note is that inevitably, a full test of Permifrost requires
privileged access to a Snowflake instance. If you do not have access to one
for testing purposes, you can sign up for a demo Snowflake account for 30 days.
Maintainers of this project also have access to a Snowflake testing account,
contact the maintainers if you require access to it as well.

### Design of Permifrost

Permifrost is not perfect and there are many things that can be improved. We'd
love your help on this. If you have any big design ideas, open an issue
so we can start the discussion. There's lots of room for improvement,
everything from the structure of the files, to the level of abstraction in
the code to how we perform tests.

## How Can I Contribute

Contributing to open-source can take many shapes!

### Reporting Bugs

Bug reports are really valuable. There are many different Snowflake configurations
out there, and it's possible we haven't caught all edge-cases. Before filing
an issue, check our [existing open issues](https://gitlab.com/gitlab-data/permifrost/-/issues)
to see if it already has been reported. If so, you can add some more colour to
the existing issue.

Otherwise, fill out a new issue. The Bug Report template has a few sections
that make it easier for us to track down the cause of the bug.

### Suggesting Features

Likewise, you can submit requests for enhancements and features by creating an
issue and selecting the Feature Request template.

### Code Contributions

You can also help by looking at our open issues, particualy those marked with
the [Good First Issue](https://gitlab.com/gitlab-data/permifrost/-/issues?label_name%5B%5D=Good+First+Issue)
label.

## Getting started with development

### Setting up the repository

There are two general 'paths' for getting changes merged into the permifrost repo:

#### Directly

The first path is to reach out to the maintainers of Permifrost & request Developer access to the repository (preferably in the dbt Slack in the #tools-permifrost channel) which will avoid cluttering the repo with unnecessary issues for adding members.

    Pros: Use GitLab Data teams CI/CD credits for the Permifrost repo, don't need to add a credit card
    Cons: Blocks initial development by new contributors until they are added to the GitLab Data/Permifrost repo

#### Forked

Second option is to fork the repo and set the new repo variable to their organization (i.e. not GitLab Data)

    Pros: Can start development immediately
    Cons: Need to have a credit card on file with GitLab (which will not be charged for the first 400 CI/CD minutes each month), need to make sure to set the repo environment variable in their fork

To work on permifrost you'll want installed, at a minimum:

- python3
- docker with docker-compose

Before starting development, you'll want to create a virtual environment
to isolate your development environment from all the other Python
work you might do on your computer. Virtual environments are one of the
most confusing parts of getting started with Python, so if the instructions
here don't make sense, or if they're not working, please ask for help!

```bash
# Run the following commands in your terminal
# First, we'll create the virutal environment
python3 -m venv env

# Next we activate it. Note, you'll have to do this every time you start a new session
source env/bin/activate

# Then we install all the dependencies needed to start working on the project,
# including pre-commit hooks
make initial-setup
```

We use [pre-commit hooks](https://pre-commit.com) to help identify simple
issues before you commit code, such as styling and linting issues.

Once you've installed the dependencies, you can run
`make test` to run the tests in a docker container.

From there, you're ready to create your first branch and start hacking on code!

```bash
# Create a branch, use a better name than this!
git switch -c my-first-branch
```

Once you're happy, push your changes and initiate a Merge Request, and your
first contribution is on its way to becoming part of the main codebase!

Not sure where to start? Many of our classes, functions, and methods lack
documentation. Adding some documentation can be a helpful way to learn
more about how permifrost works. Spend some time in the code base and you'll
see there's no standard docstring format! (Another great way to contribute,
open an issue on this and get a discussion going..) :)

This is just the tip of the iceberg. Feel free to poke around the codebase,
and make sure to look into `Makefile`, there's some fun commands in there that
will do everything from showing you test coverage to linting everything for
you. Have fun and happy hacking!
