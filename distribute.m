function varargout = distribute(opt, func, varargin)
% _________________________________________________________________________
%
%             Distribute Matlab jobs locally or on a cluster
%
% -------------------------------------------------------------------------
% ARGUMENTS
% ---------
%
% FORMAT [out1, ...] = distribute(opt, func, ('iter'/'inplace'), arg1, ...)
%
% opt  - Option structure. See 'help distribute_default'.
% func - Matlab function to apply (string or function handle)
% arg  - Arguments of the function
%        > Arguments that should be iterated over should be preceded
%          with 'iter'
%        > Arguments that should be iterated in place (i.e. the output
%          replaces the input) should be preceded with 'inplace'
% out  - Output of the function. Each one is a cell array of outputs,
%        unless some arguments were 'inplace'. In this case, the
%        function should return inplace arguments first, in the same
%        order as their input order.
% 
% -------------------------------------------------------------------------
% CONFIGURATION
% -------------
% 
% To use this kind of distributed computing, you need a workstation and a
% cluster that have both access to a shared folder. This allows the main
% script and distributed jobs to share data by writing it on disk, rather
% than using copies over SSH.
%
% You also must be able to SSH into the cluster without having to manually
% enter your password, i.e., either the cluster is not password-protected
% or your RSA key must be registered with the cluster. To do this you
% should:
% LINUX
% 1) generate your own RSA key on your workstation
%    >> ssh-keygen -t rsa
% 2) register this key with the cluster
%    >> ssh-copy-id login@cluster
%
% -------------------------------------------------------------------------
% EXAMPLE 1)
% ----------
%
% Let 'a' and 'b' be lists (cell array) of numeric arrays. We want to
% compute all sums a{i} + b{i}. We would call:
% >> c = distribute(opt, 'plus', 'iter', a, 'iter', b);
%
% It will perform the equivalent of:
% >> c = cell(size(a));
% >> for i=1:numel(a)
% >>     c{i} = plus(a{i}, b{i});
% >> end
%
% To perform the same operation in place:
% >> a = distribute(opt, 'plus', 'inplace', a, 'iter', b);
%
% which is equivalent to:
% >> for i=1:numel(a)
% >>     a{i} = plus(a{i}, b{i});
% >> end
%
% EXAMPLE 2)
% ----------
%
% Let 'dat' be a structure array. We want to apply the function 
% 'processOneData' to each of its elements. Let 'info' be a structure 
% which is useful to all of dat elements. We would call:
% >> dat = distribute(opt, 'processOneData', 'inplace', dat, info)
%
% It will perform the equivalent of:
% >> for i=1:numel(dat)
% >>     dat(i) = processOneData(dat(i), info);
% >> end
% _________________________________________________________________________

    % Parse input
    % -----------
    args  = {};
    flags = {};
    access = {};
    N = 1;
    while ~isempty(varargin)
        if ischar(varargin{1}) && any(strcmpi(varargin{1}, {'iter','inplace'}))
            flags    = [flags {varargin{1}}];
            args     = [args  {varargin{2}}];
            if iscell(varargin{2})
                access = [access  {'{}'}];
            elseif isstruct(varargin{2})
                access = [access {'()'}];
            else
                error(['distribute: an iterable input must either be ' ...
                       'a cell array or a struct array'])
            end
            if N > 1 && numel(varargin{2}) ~= N
                error(['distribute: all iterable inputs should have ' ...
                      'the same number of elements'])
            end
            N = numel(varargin{2});
            varargin = varargin(3:end);
        else
            flags    = [flags {''}];
            args     = [args  {varargin{1}}];
            access   = [access {''}];
            varargin = varargin(2:end);
        end
    end
    
    % Convert function name <-> function handle
    % -----------------------------------------
    if ischar(func)
        funcstr = func; 
        func    = str2func(func);
    else
        funcstr = func2str(func);
    end
    
    % Distribute
    % ----------
    if opt.server.setup && check_server_load(opt)
        [varargout{1:nargout}] = distribute_server(opt, funcstr, args, flags, access, N);
    elseif double(opt.client.workers) > 0
        [varargout{1:nargout}] = distribute_local(opt, func, args, flags, access, N);
    else
        [varargout{1:nargout}] = distribute_not(opt, func, args, flags, access, N);
    end

end


function ok = check_server_load(~)
    ok = true;
end

% -------------------------------------------------------------------------
%   Distribute locally
% -------------------------------------------------------------------------

function varargout = distribute_local(opt, func, args, flags, access, N)
% It is not very efficient now as all N arguments have to be sent to every
% worker. I could not find a way to be generic AND allow slicing in parfor.

    % Prepare temporary output
    % ------------------------
    Nout = nargout;
    out  = cell(N, Nout);
    
    % Iterate
    % -------
    % /!\ no efficient slicing
    parfor (n=1:N, double(opt.client.workers))
        args1 = cell(size(args));
        for i=1:numel(args)
            switch lower(flags{i})
                case ''
                    args1{i} = args{i};
                case {'iter', 'inplace'}
                    switch lower(access{i})
                        case '()'
                            args1{i} = args{i}(n);
                        case '{}'
                            args1{i} = args{i}{n};
                    end
            end
        end
        out1 = {};
        [out1{1:Nout}] = func(args1{:});
        out{n,:} = out1{:};
    end

    % Write final output
    % ------------------
    [varargout{1:nargout}] = deal({});
    j = 1;
    for i=1:numel(args)
        if strcmpi(flags{i}, 'inplace')
            varargout{j} = args{i};
            if strcmpi(access{i}, '{}')
                for n=1:N
                    varargout{j}{n} = out{n,j};
                end
            elseif strcmpi(access{i}, '()')
                for n=1:N
                    varargout{j}(n) = out{n,j};
                end
            end
            j = j + 1;
        end
    end
    j1 = j;
    for j=j1:nargout
        varargout{j} = out{:,j}';
    end
end

% -------------------------------------------------------------------------
%   Do not distribute
% -------------------------------------------------------------------------
function varargout = distribute_not(~, func, args, flags, access, N)

    % Prepare temporary output
    % ------------------------
    out = cell(N, nargout);
    
    % Iterate
    % -------
    for n=1:N
        args1 = cell(size(args));
        for i=1:numel(args)
            switch lower(flags{i})
                case ''
                    args1{i} = args{i};
                case {'iter', 'inplace'}
                    switch lower(access{i})
                        case '()'
                            args1{i} = args{i}(n);
                        case '{}'
                            args1{i} = args{i}{n};
                    end
            end
        end
        [out{n,:}] = func(args1{:});
    end

    % Write final output
    % ------------------
    [varargout{1:nargout}] = deal({});
    j = 1;
    for i=1:numel(args)
        if strcmpi(flags{i}, 'inplace')
            varargout{j} = args{i};
            if strcmpi(access{i}, '{}')
                for n=1:N
                    varargout{j}{n} = out{n,j};
                end
            elseif strcmpi(access{i}, '()')
                for n=1:N
                    varargout{j}(n) = out{n,j};
                end
            end
            j = j + 1;
        end
    end
    j1 = j;
    for j=j1:nargout
        varargout{j} = out{:,j}';
    end
end

% -------------------------------------------------------------------------
%   Distribute on server
% -------------------------------------------------------------------------

function varargout = distribute_server(opt, func, args, flags, access, N)

    % Filenames
    % ---------
    uid      = char(java.util.UUID.randomUUID()); % General UID
    mainname = ['main_' uid];
    fnames   = ['fnames_' uid '.mat'];     % job nb <-> data filename
    matin    = cell(1,N);                  % input data filenames
    matout   = cell(1,N);                  % output data filenames
    mainsh   = ['main_' uid '.sh'];        % main bash script
    mainout  = ['main_cout_' uid '.log'];  % main output file
    mainerr  = ['main_cerr_' uid '.log'];  % main error file
    
    % Write data
    % ----------
    for n=1:N
        uid1 = char(java.util.UUID.randomUUID());
        matin{n}   = ['in_' uid '_' uid1 '.mat'];
        matout{n}  = ['out_' uid '_' uid1 '.mat'];
        
        argin = cell(size(args));
        for i=1:numel(args)
            switch lower(flags{i})
                case ''
                    argin{i} = args{i};
                case {'iter', 'inplace'}
                    switch lower(access{i})
                        case '()'
                            argin{i} = args{i}(n);
                        case '{}'
                            argin{i} = args{i}{n};
                    end
            end
        end
        argin = distribute_translate(opt, argin);
        save(fullfile(opt.client.folder, matin{n}), 'argin', '-mat'); 
        clear argsin
        
    end
    save(fullfile(opt.client.folder, fnames), 'matin', 'matout', '-mat');
    
    % Write main script
    % -----------------
    % It runs each subject job
    batch_script = [             ...
        '#!' opt.sh.bin '\n'     ...
        '\n'                     ...
        '#$ -S ' opt.sh.bin '\n' ... % Shell path
        '#$ -N ' mainname '\n'   ... % Job name
        '#$ -o ' fullfile(opt.server.folder,mainout) '\n'  ... % Path to output file
        '#$ -e ' fullfile(opt.server.folder,mainerr) '\n'  ... % Path to error file
        '#$ -j n \n'                ... % Do not join out/err files
        '#$ -t 1-' num2str(N) ' \n' ... % Number of subjobs
        '\n'                        ...
        'matlab_cmd="'];
    if ~isempty(opt.matlab.priv.add)
        batch_script = [batch_script 'addpath(genpath(' opt.matlab.priv.add '));'];
    end
    batch_script = [batch_script ...
            'load(fullfile(''' opt.server.folder ''',''' fnames '''),''matin'',''matout'');' ...
            'load(fullfile(''' opt.server.folder ''',matin{$SGE_TASK_ID}),''argin'');' ...
            'argout=cell(1,' num2str(nargout) ');' ...
            'func=str2func(''' func ''');' ...
            '[argout{1:' num2str(nargout) '}]=func(argin{:});' ...
            'save(fullfile(''' opt.server.folder ''',matout{$SGE_TASK_ID}),''argout'',''-mat'');' ...
            'quit;' ...
        '"\n' ...
        opt.matlab.bin ' ' opt.matlab.opt ' -r $matlab_cmd \n' ... 
    ];
    fid = fopen(fullfile(opt.client.folder, mainsh), 'w');
    fprintf(fid, batch_script);
    fclose(fid);
    fileattrib(fullfile(opt.client.folder, mainsh), '+x', 'u');

    % Submit main script
    % ------------------
    pause(1); % necessary ??
    cmd = '';
    for i=1:numel(opt.client.source)
        cmd = [cmd 'source ' opt.client.source{i} ' >/dev/null 2>&1 ; '];
    end
    cmd = [cmd opt.ssh.bin ' ' opt.server.login '@' opt.server.ip ' "'];
    for i=1:numel(opt.server.source)
        cmd = [cmd 'source ' opt.server.source{i} ' >/dev/null 2>&1 ; '];
    end
    cmd = [cmd opt.sched.sub ' '];
    switch lower(opt.sched.type)
        case 'sge'
            cmd = [cmd ' -l vf=' num2str(opt.job.mem) ...
                       ' -l h_vmem=' num2str(opt.job.mem) ' '];
        otherwise
            error('distribute: scheduler %s not implemented yet', opt.sched.type);
    end
    cmd = [cmd fullfile(opt.server.folder, mainsh) '"'];
    [status, result] = system(cmd);
    if status
        fprintf([result '\n']);
        error('distribute: status ~= 0 for main on server!')
    end
    fprintf(result);
    
    jobid = result(15:20); % ID of array job on server
    
    % Track jobs
    % ----------
    start_track = tic;
    cmd = '';
    for i=1:numel(opt.client.source)
        cmd = [cmd 'source ' opt.client.source{i} ' >/dev/null 2>&1 ; '];
    end
    cmd = [cmd opt.ssh.bin ' ' opt.server.login '@' opt.server.ip ' "'];
    for i=1:numel(opt.server.source)
        cmd = [cmd 'source ' opt.server.source{i} ' >/dev/null 2>&1 ; '];
    end
    cmd = [cmd opt.sched.stat ' '];
    switch lower(opt.sched.type)
        case 'sge'
            cmd = [cmd ' | grep ' jobid ' '];
        otherwise
            error('distribute: scheduler %s not implemented yet', opt.sched.type);
    end
    cmd = [cmd '"'];
    while 1
        pause(2); % Do not refresh too often
        [~, result] = system(cmd);
        if isempty(result)
            if opt.verbose
                fprintf('%d jobs processed in %0.1fs\n', N, toc(start_track))
            end
            break
        end
    end
    
    % Read output
    % -----------
    pause(1)
    % Reverse translation
    opt.server_to_client = true;
    % initialise output structure
    [varargout{1:nargout}] = deal({});
    j = 1;
    for i=1:numel(args)
        if strcmpi(flags{i}, 'inplace')
            varargout{j} = args{i};
            j = j + 1;
        end
    end
    % fill output structure
    for n=1:N
        % read argout
        if ~exist(fullfile(opt.client.folder, matout{n}), 'file')
            warning('File nb %d (%s) still does not exist.', ...
                    n, fullfile(opt.client.folder, matout{n}))
            continue;
        end
        load(fullfile(opt.client.folder, matout{n}), 'argout');
        argout = distribute_translate(opt, argout);
        % fill inplace
        j = 1;
        for i=1:numel(args)
            if strcmpi(flags{i}, 'inplace')
                if strcmpi(access{i}, '{}')
                    varargout{j}{n} = argout{j};
                elseif strcmpi(access{i}, '()')
                    varargout{j}(n) = argout{j};
                end
                j = j + 1;
            end
        end
        % fill remaining
        j1 = j;
        for j=j1:nargout
            varargout{j}{n} = argout{j};
        end
        clear argout
    end
    opt.server_to_client = false;
    
    % Clean disk
    % ----------
    if opt.clean
        names = [{mainsh mainout mainerr ...
                 dummysh dummyout dummyerr ...
                 fnames} matin matout];
        for i=1:numel(names)
            if exist(fullfile(opt.client.folder, names{i}), 'file')
                delete(fullfile(opt.client.folder, names{i}));
            end
        end
    end
    
end