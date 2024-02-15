# Grevling

*Grevling* (meaning "badger" in Norwegian) is a highly configurable tool for

- running *parametrized* jobs,
- collecting outputs from these jobs,
- storing relevant results in a database, and
- querying and reporting based on the database.

Grevling is both a command-line tool as well as a Python library that provides
an API to all the above.

## Overview

A Grevling *case* is defined by a configuration file, written in the [Gold
language](https://github.com/TheBB/Gold). Gold is a programmable configuration
lanaguage created explicitly for use with Grevling. This configuration file
is usually named `grevling.gold` and it defines, among other things:

- the parameters relevant to your case, their types and valid ranges,
- how to set up an instance of your job in an isolated environment,
- how to run the job, and
- which results to collect and how to interpret them.
