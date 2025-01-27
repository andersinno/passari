Installation
============

Installation using virtualenv
-----------------------------

Install Passari
^^^^^^^^^^^^^^^

It is recommended to install Passari and other related software using *virtualenv*, which prevents possible problems with the system's package manager.

To get started, ensure that Python 3.6+ is installed. On CentOS 7, you can usually get started by installing the required tools using `yum`:

.. code-block:: console

    $ yum install python36-libs python36-devel


After this, create a new directory that will contain your virtualenv:

.. code-block:: console

    $ python3.6 -mvenv <venv_dir>
    $ source <venv_dir>/bin/activate
    # Replace 1.1 with the latest git-tagged version to install the latest version
    $ pip install git+https://github.com/finnish-heritage-agency/passari.git@1.1#egg=passari

You can disable the *virtualenv* using `deactivate` and activate it with `source <venv_dir>/bin/activate`.

Install dpres-siptools-ng
^^^^^^^^^^^^^^^^^^^^^^^^^

You will also need to install CSC packaging and validation tools. Read `the official instructions <https://github.com/Digital-Preservation-Finland/dpres-siptools-ng>`_ for details.

Once you have installed *Passari* and *dpres-siptools-ng*, run a command to test that the installation succeeded:

.. code-block:: console

    $ download-object --help
