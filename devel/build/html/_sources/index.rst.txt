.. Grevling documentation master file, created by
   sphinx-quickstart on Wed Aug  4 11:08:00 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.


Grevling
========

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Contents:

   getting-started


Welcome to *Grevling*.  Grevling (the Norwegian word for "Badger") is a tool for
parametrizing workflows.  Think of Grevling as solving a problem similar to
Make.


Typical use case
----------------

You're a scientist working with a numerical simulation tool.  This tool is
impressive, it has a large variety of options and a daunting but well-structured
input file format.  You wish to generate and simulate a large family of similar
but different cases.  Many of these cases require pre- and post-processing steps
as well.  In addition, you want to collect information from each run and store
them in a table, maybe also make some plots.

At some point, setting up the input for each case by hand becomes intractable.
You could script it, say, in Bash, but Bash is nobody's favorite tool.  You
already have a number of such scripts lying around, each of them with subtle
differences, and you can't remember which one does what anyway.

Instead, you can configure your setup with Grevling.  Grevling will be able to:

- generate the inputs for each case
- run the simulator with pre- and post-processing
- gather data from each run and collect them in a data frame
- produce plots


Design goals
------------

Grevling should

- never require support from third-party tools: it should not be necessary for
  other programs to know that they are being run by Grevling,
- be fully declarative: no more ad-hoc scripting (although technically you can
  this in Grevling, the design should not encourage it).


.. Indices and tables
.. ==================

.. * :ref:`genindex`
.. * :ref:`modindex`
.. * :ref:`search`
