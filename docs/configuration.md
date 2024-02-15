# Configuration

Grevling configuration is written in the
[Gold language](https://thebb.github.io/Gold).
This is a *programmable configuration language*, meaning that it supports
conventional programming language features such as defining functions, sharing
and importing data to and from other files, and so on. In this context, a
`grevling.gold` file is a *program* whose output is a JSON-like object, which is
the actual configuration read by Grevling.


## Specifying the parameter space

Grevling runs *parametrized* jobs, so it must know what the parameters are for
each job that it runs. The `parameters` key, if present, maps *names of
parameters* to *lists of possible values*, like so:

```
{
    parameters: {
        degree: [1, 2, 3, 4, 5, 6],
        meshsize: [1, 0.5, 0.25, 0.0125],
        adaptive: [true, false],
    },
}
```

This will run the job 48 times in total, once for each combination of
`degree`, `meshsize` and `adaptive`.

Grevling has functions that help facilitate some common use cases. They can be
imported from the `"grevling"` library - a library that is available for import
when running with Grevling. This is equivalent to the above:

```
import "grevling" as g

{
    parameters: {
        degree: g.linspace(1, 6, npts: 6),
        meshsize: g.gradspace(1, 0.0125, npts: 4, grading: 0.5),
        adaptive: [true, false],
    }
}
```

The function `linspace` creates a uniform sampling of an interval with the given
number of points. The function `gradspace` creates a geometric sampling that is
denser in one end than in the other. Each successive subdivision is longer or
shorter than the previous by the factor indicated by `grading`.


## Preparing a job

Grevling runs each instance of a job in a temporary directory. Often this
directory must be prepared before the job runs by copying some files from the
source directory (the directory where the configuration file is located). This
can be configured using the `prefiles` key, which should be a list of files to
copy. We recommend using the `copy` function to construct this list.

```
import "grevling" as { copy }

{
    prefiles: [
        copy("some-file.txt"),
        copy("some-other-file.txt"),
    ]
}
```

The files will then be copied into the working directory before the job runs.
The name of the file in the working directory will be identical to its name in
the source directory, but the name (and location) can be changed with an
optional second argument:

```
import "grevling" as { copy }

{
    prefiles: [
        copy("some-file.txt", "subpath/somewhere-else.txt"),
    ]
}
```

Grevling also supports *globbing*: copying multiple files at once. For this, use
the `glob` function.

```
import "grevling" as { glob }

{
    prefiles: [
        glob("*.jpg", "images/"),
    ],
}
```

In the above example, all *jpg* files will be copied into the *images*
subdirectory in the working directory. Beware that all files will retain their
original relative path to the target. In the following example, the files will
end up in `images/subpath/...`.

```
import "grevling" as { glob }

{
    prefiles: [
        glob("subpath/*.jpg", "images/"),
    ],
}
```

Often, files will need to be modified in some way according to the values of the
parameters. For this, Grevling uses template substitution with the
[Mako](https://www.makotemplates.org/) library. To enable template substitution,
set the optional `template` keyword argument to `true`.

```
import "grevling" as { copy }

{
    prefiles: [
        copy("input.ini", template: true),
    ]
}
```

Please refer to the
[Mako documentation](https://docs.makotemplates.org/en/latest/syntax.html)
for more help on writing Mako templates. The values of all parameters will be
available to the template when rendering, as well as the output of the
`evaluate` function, if present (see [extra evalution](#extra-evaluation)).

TODO: Glob


## Running a job

The `script` key indicates how to run a job after it has been prepared. It is a
list of commands. We recommend using the `cmd` function to construct this list.

```
import "grevling" as { cmd }

{
    script: [
        cmd("some-program --args ..."),
        cmd("some-other-program --more-args ..."),
    ]
}
```

Grevling will the execute each command in sequence, aborting immediately if one
of them fails.

Grevling also supports running a command as a list of arguments (the first
element being the command to run, the subsequent elements being the arguments).
This option is more foolproof against accidental shell quoting trouble, and is
generally recommended - although somewhat more verbose.

```
import "grevling" as { cmd }

{
    script: [
        cmd(["some-program", "--args", ...]),
        cmd(["some-other-program", "--more-args", ...]),
    ]
}
```

The `cmd` function takes many optional parameters:

```
cmd(
    command;
    name = null,
    env = {},
    container = null,
    container_args = [],
    allow_failure = false,
    retry_on_fail = false,
    capture = [],
    workdir = null,
)
```

- *name*: To access information about command output after a job has run, it is
  necessary for each command to have its own uniuqe name. If this is not
  provided, Grevling will attempt to deduce the name from the *command*
  parameter, but it may be difficult or impossible to do so in certain
  circumstances. For best results, always name your commands.
- *env*: Environment variables that will be used to augment the environment of
  the command when it runs.
- *container* and *container_args*: See TODO.
- *allow_failure*: If this option is enabled, Grevling will not abort the job if
  this command fails.
- *retry_on_fail*: If this option is enabled, Grevling will re-run the command
  if it fails. Note that this continues *indefinitely* if the command continues
  to fail.
- *capture*: Specifications for capturing output from the command's stdout
  stream. TODO.
- *workdir*: Use this option to allow the command to run in a different working
  directory. This path is NOT relative to the actual working directory!


## Collecting output

After a job isntance has finished successfully, Grevling copies some information
back from the working directory to its own internal database (conventionally
located in the `.grevlingdata` subdirectory wherever the configuration file is
located). By default, Grevling copies the stdout and stderr stream from each
command, as well as some metadata from the job instance in question. However,
often you'd like to collect more data. For this, use the `postfiles` key. It
works identically to the `prefiles` key (see
[preparing a job](#preparing-a-job)), except it does not support templates.

Use this to collect output files that you are interested in keeping.

Note that files declared in `prefiles` will not automatically be copied back
after the job instance is finished. If you want to keep those files, e.g. to
check whether template substitution was successful, those files must be copied
explicitly in `postfiles` as well.

```
import "grevling" as { copy }

{
    postfiles: [
        copy("some-output.dat"),
        copy("input.ini"),
    ]
}
```


## Parameter-dependent execution

As a parametrized job runner, Grevling allows almost any step of the job running
process to depend on the value of one or more parameters. As we have seen above,
files can undergo template substitution, but that is not all. Most entries in
the Grevling configuration file can be *functions* that accept parameters as
input and return the necessary values.

For example, assume we have an `adaptive` parameter as such:

```
{
    parameters: {
        adaptive: [true, false],
    }
}
```

Let us also assume that our command requires an input file that is different
when `adaptive` is true as opposed to false, and let us also assume that this
difference is great enough that we are not interested in using template
substitution to solve the problem - or perhaps that the input file is binary,
and thus template substitution will not work.

Instead we could make `prefiles` a function that determines what file to use:

```
import "grevling" as { copy }

{
    prefiles: {|adaptive|} [
        copy(
            if adaptive
                then "input-adaptive.dat"
                else "input-nonadaptive.dat",
            "input.dat"
        ),
    ]
}
```

During job preparation, Grevling will call this function with the parameter
`adaptive` as a keyword argument. The return value will then be used as the job
preparation schema for that job instance. In this case, it indicates that either
*input-adaptive.dat* or *input-nonadaptive.dat* should be copied to *input.dat*
in the working directory, depending on the value of `adaptive`.

Note that *all parameters* as well as TODO will be provided as keyword
arguments. In Gold, it is not an error if a function is called with more keyword
arguments than it accepts, thus the above will work even if more parameters are
added: a function may accept only those it requires.

All of the `prefiles`, `postfiles` and `script` keys support this mechanism. It
can be used, for example, to allow command-line arguments to some commands to be
parameter-dependent:

```
import "grevling" as { cmd }

{
    script: {|adaptive|} [
        cmd([
            "some-command",
            if adaptive then "--adaptive" else "--no-adaptive",
        ]),
    ]
}
```

or perhaps this, making use of Gold's
[advanced collection features](https://thebb.github.io/Gold/whirlwind/#advanced-collections).

```
import "grevling" as { cmd }

{
    script: {|adaptive|} [
        cmd([
            "some-command",
            if adaptive: "--adaptive",
        ]),
    ]
}
```


## Extra evaluation

Often it is necessary to perform extra evalution of certain parameter-dependent
quantities in a central location. For this purpose, Grevling offers the
`evaluate` key, which should be a function of the parameters, and which returns
an object of extra values. Those extra values can be used as input to other
functions (such as `prefiles`, `postfiles` and `script`) - as if they were
parameters, and they are also collected in the Grevling database as part of the
evaluation context TODO.

Consider for example:

```
{
    parameters: {
        degree: [1, 2, 3, 4, 5, 6],
        meshsize: [1, 0.5, 0.25, 0.0125],
        dimension: [1, 2, 3],
    },
}
```

In certain numerical simulations, a relevant quantity of interest might be the
number of nodes in a mesh, which is

```
    ((1 / meshsize) + degree) ^ dimension
```

This formula can of course be replicated everywhere it might be needed:

- in templates
- in functions like `prefiles`, `postfiles` and `script`
- when querying the database for results after running the job

but it is tedious and error-prone to do so. Nor is it really a solution to
parametrize the problem in terms of this quantity. To solve this, we can write:

```
{
    parameters: {
        degree: [1, 2, 3, 4, 5, 6],
        meshsize: [1, 0.5, 0.25, 0.0125],
        dimension: [1, 2, 3],
    },
    evaluate: {|degree, meshsize, dimension|} {
        numnodes: ((1 / meshsize) + degree) ^ dimension,
    },
}
```

Now, `numnodes` will be available in all those contexts mentioned earlier on the
same level as the parameters. For instance, you could write:

```
import "grevling" as { copy }

{
    prefiles: {|numnodes|}: [
        copy("mesh.dat", "mesh-${nnodes}.dat"),
    ]
}
```
