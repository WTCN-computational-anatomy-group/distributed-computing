# Distributed Computing in Matlab

This project contains a set of functions to help distribute the execution of Matlab functions on a cluster.

## Prerequisites

- The cluster (i.e. "the server") and the main workstation (i.e., "the client") should to be able to access some shared disk space. It is usually possible to do so by mounting a network folder on both stations. If the server and client do not share the same network, mounting can be performed over ssh with sshfs ([Linux](https://doc.ubuntu-fr.org/sshfs), [MacOS](https://osxfuse.github.io), [Windows](https://github.com/Foreveryone-cz/win-sshfs))

- The client should have an SSH software installed. Both [ssh](https://doc.ubuntu-fr.org/ssh), for linux, and [PuTTY](http://www.putty.org), for Windows, are currently handled.

- The client should be able to connect to the cluster without having to type a password. This is usually managed by registering RSA keys on the cluster. On linux, this is done by

    1) Generate a set of public and private keys
    ```
    ssh-keygen -t rsa
    ```

    2) Register the key with the cluster
    ```
    ssh-copy-id login@cluster
    ```

- Matlab should be installed on the server, with an infinite (or large) number of licenses.

### Limitations

- For now, we only support clusters managed with Sun Grid Engine. It should also work with its forks ([OGS](http://gridscheduler.sourceforge.net), ...) but they were not tested. We plan on supporting other queuing systems, such as PBS, in the future?

## Usage

### Configuration

First an option structure must be generated with the `distribute_default` function. Beforehand, a number of mandatory options should be manually set (else, jobs wil be run locally).

Here is an example of a typical configuration:
```
dist = struct;

dist.server.ip      = 'cluster.university.ac.uk';
dist.server.login   = 'me';
dist.server.folder  = '/home/me/distribute';
dist.client.folder  = '/Users/me/distribute';

dist.matlab.bin     = '/share/apps/matlab';

dist.translate      = {'/Users/me/mydata' '/home/me/data'};

dist = distribute_default(dist);
```

### Options

Several additional options can be set in order to specify the cluster configuration more precisely, or to load matlab packages at runtime. Here is the complete list:

#### CLUSTER
```
server.ip     - IP adress (or alias name) of the cluster ['' = no cluster]
server.login  - Login with which to connect ['']
server.source - Files to source on server side [auto > Try to find bashrc and/or bash_profile]
server.folder - Shared folder for writing data, scripts, etc.
```

#### LOCAL
```
client.source  - Files to source on server side [auto]
client.workers - Number of local workers [auto]
client.folder  - Shared folder for writing data, scripts, etc.
```

#### SUBMIT JOBS
```
ssh.type    - SSH software to use 'ssh'/'putty'/[try to detect]
ssh.bin     - Path to the ssh binary [try to detect]
sched.sub   - Path to the submit binary [try to detect]
sched.stat  - Path to the stat binary [try to detect]
sched.type  - Type of scheduler 'sge'/'pbs'/[try to detect]
job.mem     - Max memory usage by a single job ['2G']
optim.optim - Try to optimise distribution between cluster and local [true]
optim.busy  - Business threshold for which local is preferred over
              cluster [0.9]
```

#### MATLAB
```
matlab.bin    - Path to matlab binary [try to detect]
matlab.add    - Paths to add to Matlab path [{}]
matlab.opt    - Commandline options to pass to matlab
                [{'-nojvm' '-nodesktop' '-nosplash' '-singleCompThread'}]
spm.path      - Path to SPM [try to detect]
spm.toolboxes - List of SPM toolboxes to add to Matlab path [{}]
```

#### DATA
```
translate - Cell array of size 2xN with translation between client and
            server paths [{client.folder server.folder}].
            Example:
                 {'/home/me/'     '/mnt/users/me/' ;
                  '/shared/data/' '/mnt/shared/data'}
restrict  - Restrict translation to a class: 'char'/'file_array'/['']
clean     - Clean tmp data when finished [true]
```

### Run

The main function is `distribute`. Its syntax, is quite straightforward: it takes the option structure, a function name or handle, and the list of arguments to pass to the function. Arguments that should be sliced (i.e., iterated over), should be preceded by `'iter'`. Arguments that should be sliced *and* are both inputs and outputs (in particular, structure arrays) should be preceded by `'inplace'`.

```
FORMAT [out1, ...] = distribute(opt, func, ('iter'/'inplace'), arg1, ...)

opt  - Option structure. See 'help distribute_default'.
func - Matlab function to apply (string or function handle)
arg  - Arguments of the function
       > Arguments that should be iterated over should be preceded
         with 'iter'
       > Arguments that should be iterated in place (i.e. the output
         replaces the input) should be preceded with 'inplace'
out  - Output of the function. Each one is a cell array of outputs,
       unless some arguments were 'inplace'. In this case, the
       function should return inplace arguments first, in the same
       order as their input order.
```

### Examples

Let us give a first use case, were we have a list of pairs of arrays that should be summed:
```
% Initialise arrays
N   = 10;
DIM = [5 5];
a   = cell(1,N);
b   = cell(1,N);
for i=1:N
    a{i} = randn(DIM);
    b{i} = randn(DIM);
end

% Local processing
true_c = cellfun(@plus, a, b, 'UniformOutput', false);

% Distributed processing
dist_c = distribute(dist, 'plus', 'iter', a, 'iter', b);
```

Let us now apply a distributed process to a structure array, which is both input and output
```
% Initialise structure array
N = 10;
f = cell(1,N);
a = struct('f', f);

% Set the value 3 in all fields 'f'
a = distribute(dist, @setfield, 'inplace', a, 'f', 3);
```

## Future developments

We intend to allow:
- job batching, where a single job processes several "subjects".
- automatic detection of RAM usage.
- optimising cluster use by choosing between local and distributed processing based on the cluster load.
- distributing scripts/binaries on top of Matlab functions. This can be helpful for working with compiled Matlab scripts.
