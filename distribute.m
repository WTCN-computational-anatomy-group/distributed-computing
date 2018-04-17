function varargout = distribute(opt, func, varargin)
% _________________________________________________________________________
%
%             Distribute Matlab jobs locally or on a cluster
%
% -------------------------------------------------------------------------
% ARGUMENTS
% ---------
%
% FORMAT [opt,out1, ...] = distribute(opt, func, ('iter'/'inplace'), arg1, ...)
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
% >> [opt,c] = distribute(opt, 'plus', 'iter', a, 'iter', b);
%
% It will perform the equivalent of:
% >> c = cell(size(a));
% >> for i=1:numel(a)
% >>     c{i} = plus(a{i}, b{i});
% >> end
%
% To perform the same operation in place:
% >> [opt,a] = distribute(opt, 'plus', 'inplace', a, 'iter', b);
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
% >> [opt,dat] = distribute(opt, 'processOneData', 'inplace', dat, info)
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
        if opt.job.batch
            [varargout{1:nargout}] = distribute_server_batch(opt, funcstr, args, flags, access, N);
        else
            [varargout{1:nargout}] = distribute_server_ind(opt, funcstr, args, flags, access, N);
        end
        
        opt = varargout{1};
        
        if opt.job.est_mem
            % Estimate new memory usage
            % ----------
            opt = estimate_mem(opt,N);            
        end
    elseif double(opt.client.workers) > 0
        [varargout{2:nargout}] = distribute_local(opt, func, args, flags, access, N);
    else
        [varargout{2:nargout}] = distribute_not(opt, func, args, flags, access, N);
    end

    varargout{1} = opt;    
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
%   Distribute on server (batch)
% -------------------------------------------------------------------------

function varargout = distribute_server_batch(opt, func, args, flags, access, N)

    % Filenames
    % ---------
    if exist('java.util.UUID', 'class')
        uid      = char(java.util.UUID.randomUUID()); % General UID
    else
        uid = datestr(now, 'yyyymmddTHHMMSS');
    end
    mainname = ['main_' uid];
    fnames   = ['fnames_' uid '.mat'];     % job nb <-> data filename
    matin    = cell(1,N);                  % input data filenames
    matout   = cell(1,N);                  % output data filenames
    mainsh   = ['main_' uid '.sh'];        % main bash script
    mainout  = ['main_cout_' uid '.log'];  % main output file
    mainerr  = ['main_cerr_' uid '.log'];  % main error file
    
    if opt.job.use_dummy
        [~,dummy_sh,dummy_out,dummy_err] = create_dummy_job(opt);
    end
    
    % Write data
    % ----------
    for n=1:N
        if exist('java.util.UUID', 'class')
            uid1 = char(java.util.UUID.randomUUID());
        else
            uid1 = num2str(n);
        end
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
        clear argin
        
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
        batch_script = [batch_script 'addpath(' opt.matlab.priv.add ');'];
    end
    if ~isempty(opt.matlab.priv.addsub)
        batch_script = [batch_script 'addpath(' opt.matlab.priv.addsub ');'];
    end
    batch_script = [batch_script ...
            'load(fullfile(''' opt.server.folder ''',''' fnames '''),''matin'',''matout'');' ...
            'load(fullfile(''' opt.server.folder ''',matin{$SGE_TASK_ID}),''argin'');' ...
            'argout=cell(1,' num2str(nargout - 1) ');' ...
            'func=str2func(''' func ''');' ...
            '[argout{1:' num2str(nargout - 1) '}]=func(argin{:});' ...
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
    cmd = '';
    for i=1:numel(opt.client.source)
        cmd = [cmd 'source ' opt.client.source{i} ' >/dev/null 2>&1 ; '];
    end
    cmd = [cmd opt.ssh.bin ' ' opt.ssh.opt ' ' opt.server.login '@' opt.server.ip ' "'];
    for i=1:numel(opt.server.source)
        cmd = [cmd 'source ' opt.server.source{i} ' >/dev/null 2>&1 ; '];
    end
    cmd = [cmd opt.sched.sub ' '];
    switch lower(opt.sched.type)
        case 'sge'
            cmd = [cmd ' -l vf=' num2str(opt.job.mem{1}) ...
                       ' -l h_vmem=' num2str(opt.job.mem{1}) ' '];
        otherwise
            error('distribute: scheduler %s not implemented yet', opt.sched.type);
    end
    cmd = [cmd fullfile(opt.server.folder, mainsh) '"'];
    [status, result] = system(cmd);
    if status
        fprintf([result '\n']);
        error('distribute: status ~= 0 for main on server!')
    end
    
    s             = regexp(result, '^\D*(?<id>\d+)', 'names'); % ID of array job on server    
    opt.job.id{1} = s.id;
        
    fprintf_job(opt,N);
        
    if opt.job.use_dummy
        % Submit dummy job
        % ----------
        cmd = '';
        for i=1:numel(opt.client.source)
            cmd = [cmd 'source ' opt.client.source{i} ' >/dev/null 2>&1 ; '];
        end
        cmd = [cmd opt.ssh.bin ' ' opt.server.login '@' opt.server.ip ' "'];
        for i=1:numel(opt.server.source)
            cmd = [cmd 'source ' opt.server.source{i} ' >/dev/null 2>&1 ; '];
        end
        cmd = [cmd opt.sched.sub ' '];
        
        cmd = [cmd ' -l vf=0.1G -l h_vmem=0.1G -hold_jid ' mainname ' -cwd ' fullfile(opt.server.folder,dummy_sh) '"'];
                
        [status,result] = system(cmd);
        if status
            fprintf([result '\n'])
            error('status~=0 for dummy job on Holly!') 
        end
        
        s        = regexp(result, '^\D*(?<id>\d+)', 'names'); % ID of array job on server    
        dummy_id = s.id;
    end
    
    % Track jobs
    % ----------
    cmd = '';
    for i=1:numel(opt.client.source)
        cmd = [cmd 'source ' opt.client.source{i} ' >/dev/null 2>&1 ; '];
    end
    cmd = [cmd opt.ssh.bin ' ' opt.ssh.opt ' ' opt.server.login '@' opt.server.ip ' "'];
    for i=1:numel(opt.server.source)
        cmd = [cmd 'source ' opt.server.source{i} ' >/dev/null 2>&1 ; '];
    end
    cmd = [cmd opt.sched.stat ' '];       
    if opt.job.use_dummy
        cmd = [cmd ' | grep ' dummy_id ' '];
    else
        switch lower(opt.sched.type)
            case 'sge'
                cmd = [cmd ' | grep ' opt.job.id{1} ' '];
            otherwise
                error('distribute: scheduler %s not implemented yet', opt.sched.type);
        end
    end
    cmd = [cmd '"'];
    
    start_track = tic;
    while 1
        pause(10); % Do not refresh too often
        
        [~, result] = system(cmd);
        
        if isempty(result)            
            fprintf_job(opt,N,toc(start_track));                            
            break
        end
    end
    
    % Store opt
    %-----------
    varargout{1} = opt;
    
    % Read output
    % -----------
    pause(1)
    % Reverse translation
    opt.server_to_client = true;
    % initialise output structure
    [varargout{2:nargout}] = deal({});
    j = 2;
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
        j = 2;
        for i=1:numel(args)
            if strcmpi(flags{i}, 'inplace')
                if strcmpi(access{i}, '{}')
                    varargout{j}{n} = argout{j - 1};
                elseif strcmpi(access{i}, '()')
                    varargout{j}(n) = argout{j - 1};
                end
                j = j + 1;
            end
        end
        % fill remaining
        j1 = j;
        for j=j1:nargout
            varargout{j}{n} = argout{j - 1};
        end
        clear argout
    end
    opt.server_to_client = false;
    
    % Clean disk
    % ----------
    if opt.clean
        names = [{mainsh mainout mainerr ...
                 fnames} matin matout];
        if opt.job.use_dummy
            names = [names {dummy_sh dummy_out dummy_err}];
        end
        for i=1:numel(names)
            if exist(fullfile(opt.client.folder, names{i}), 'file')
                delete(fullfile(opt.client.folder, names{i}));
            end
        end
    end
    
end

% -------------------------------------------------------------------------
%   Distribute on server (individual)
% -------------------------------------------------------------------------

function varargout = distribute_server_ind(opt, func, args, flags, access, N)

    if numel(opt.job.mem) == 1
        [opt.job.mem{1:N}] = deal(opt.job.mem{1});
    end

    if opt.job.use_dummy
        [~,dummy_sh,dummy_out,dummy_err] = create_dummy_job(opt);
    end
    
    mainname = cell(1,N);
    mainsh   = cell(1,N);
    mainout  = cell(1,N);
    mainerr  = cell(1,N);
    matin    = cell(1,N);
    matout   = cell(1,N);
    if exist('java.util.UUID', 'class')
        uid1 = ''; % General UID
    else
        uid1 = datestr(now, 'yyyymmddTHHMMSS');
    end
    for n=1:N

        % Filenames
        % ---------
        if exist('java.util.UUID', 'class')
            uid = char(java.util.UUID.randomUUID());
        else
            uid = [uid1 '_' num2str(n)];
        end
        mainname{n} = ['main_' uid];
        mainsh{n}   = ['main_' uid '.sh'];        % main bash script
        mainout{n}  = ['main_cout_' uid '.log'];  % main output file
        mainerr{n}  = ['main_cerr_' uid '.log'];  % main error file
        matin{n}    = ['in_' uid '.mat'];
        matout{n}   = ['out_' uid '.mat'];
    
        % Write data
        % ----------
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
        clear argin
    
        % Write main script
        % -----------------
        % It runs each subject job
        batch_script = [             ...
            '#!' opt.sh.bin '\n'     ...
            '\n'                     ...
            '#$ -S ' opt.sh.bin '\n' ... % Shell path
            '#$ -N ' mainname{n} '\n'   ... % Job name
            '#$ -o ' fullfile(opt.server.folder,mainout{n}) '\n'  ... % Path to output file
            '#$ -e ' fullfile(opt.server.folder,mainerr{n}) '\n'  ... % Path to error file
            '#$ -j n \n'                ... % Do not join out/err files
            '\n'                        ...
            'matlab_cmd="'];
        if ~isempty(opt.matlab.priv.add)
            batch_script = [batch_script 'addpath(' opt.matlab.priv.add ');'];
        end
        if ~isempty(opt.matlab.priv.addsub)
            batch_script = [batch_script 'addpath(' opt.matlab.priv.addsub ');'];
        end
        batch_script = [batch_script ...
                'load(fullfile(''' opt.server.folder ''',''' matin{n} '''),''argin'');' ...
                'argout=cell(1,' num2str(nargout - 1) ');' ...
                'func=str2func(''' func ''');' ...
                '[argout{1:' num2str(nargout - 1) '}]=func(argin{:});' ...
                'save(fullfile(''' opt.server.folder ''',''' matout{n} '''),''argout'',''-mat'');' ...
                'quit;' ...
            '"\n' ...
            opt.matlab.bin ' ' opt.matlab.opt ' -r $matlab_cmd \n' ... 
        ];
        fid = fopen(fullfile(opt.client.folder, mainsh{n}), 'w');
        fprintf(fid, batch_script);
        fclose(fid);
        fileattrib(fullfile(opt.client.folder, mainsh{n}), '+x', 'u');

        % Submit main script
        % ------------------
        cmd = '';
        for i=1:numel(opt.client.source)
            cmd = [cmd 'source ' opt.client.source{i} ' >/dev/null 2>&1 ; '];
        end
        cmd = [cmd opt.ssh.bin ' ' opt.ssh.opt ' ' opt.server.login '@' opt.server.ip ' "'];
        for i=1:numel(opt.server.source)
            cmd = [cmd 'source ' opt.server.source{i} ' >/dev/null 2>&1 ; '];
        end
        cmd = [cmd opt.sched.sub ' '];
        switch lower(opt.sched.type)
            case 'sge'
                cmd = [cmd ' -l vf=' num2str(opt.job.mem{n}) ...
                           ' -l h_vmem=' num2str(opt.job.mem{n}) ' '];
            otherwise
                error('distribute: scheduler %s not implemented yet', opt.sched.type);
        end
        cmd = [cmd fullfile(opt.server.folder, mainsh{n}) '"'];
        [status, result] = system(cmd);
        if status
            fprintf([result '\n']);
            error('distribute: status ~= 0 for job %d on server!', n)
        end        

        s             = regexp(result, '^\D*(?<id>\d+)', 'names'); % ID of array job on server
        opt.job.id{n} = s.id;
    
    end
        
    fprintf_job(opt,N);
    
    if opt.job.use_dummy
        % Submit dummy job
        % ----------                
        cmd = '';
        for i=1:numel(opt.client.source)
            cmd = [cmd 'source ' opt.client.source{i} ' >/dev/null 2>&1 ; '];
        end
        cmd = [cmd opt.ssh.bin ' ' opt.server.login '@' opt.server.ip ' "'];
        for i=1:numel(opt.server.source)
            cmd = [cmd 'source ' opt.server.source{i} ' >/dev/null 2>&1 ; '];
        end
        cmd = [cmd opt.sched.sub ' '];
        
        nam = mainname{1};
        for n=2:N
            nam = [nam ',' mainname{n}];
        end
        
        cmd = [cmd ' -l vf=0.1G -l h_vmem=0.1G -hold_jid ' nam ' -cwd ' fullfile(opt.server.folder,dummy_sh) '"'];
                
        [status,result] = system(cmd);
        if status
            fprintf([result '\n'])
            error('status~=0 for dummy job on Holly!') 
        end
        
        s        = regexp(result, '^\D*(?<id>\d+)', 'names'); % ID of array job on server    
        dummy_id = s.id;
    end
    
    % Track jobs
    % ----------    
    cmd = '';
    for i=1:numel(opt.client.source)
        cmd = [cmd 'source ' opt.client.source{i} ' >/dev/null 2>&1 ; '];
    end
    cmd = [cmd opt.ssh.bin ' ' opt.ssh.opt ' ' opt.server.login '@' opt.server.ip ' "'];
    for i=1:numel(opt.server.source)
        cmd = [cmd 'source ' opt.server.source{i} ' >/dev/null 2>&1 ; '];
    end
    cmd = [cmd opt.sched.stat ' '];
    if opt.job.use_dummy
        cmd = [cmd ' | grep ' dummy_id ' '];
    else
        switch lower(opt.sched.type)
            case 'sge'
                cmd = [cmd ' | grep '];
                for n=1:N
                    cmd = [cmd '-e ' opt.job.id{n} ' '];
                end
            otherwise
                error('distribute: scheduler %s not implemented yet', opt.sched.type);
        end
    end
    cmd = [cmd '"'];
    
    start_track = tic;
    while 1
        pause(10); % Do not refresh too often
        
        [~, result] = system(cmd);
        
        if isempty(result)            
            fprintf_job(opt,N,toc(start_track));           
            
            break
        end
    end
    
    % Store opt
    %-----------
    varargout{1} = opt;
    
    % Read output
    % -----------
    pause(1)
    % Reverse translation
    opt.server_to_client = true;
    % initialise output structure
    [varargout{2:nargout}] = deal({});
    j = 2;
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
        j = 2;
        for i=1:numel(args)
            if strcmpi(flags{i}, 'inplace')
                if strcmpi(access{i}, '{}')
                    varargout{j}{n} = argout{j - 1};
                elseif strcmpi(access{i}, '()')
                    varargout{j}(n) = argout{j - 1};
                end
                j = j + 1;
            end
        end
        % fill remaining
        j1 = j;
        for j=j1:nargout
            varargout{j}{n} = argout{j - 1};
        end
        clear argout
    end
    opt.server_to_client = false;
    
    % Clean disk
    % ----------
    if opt.clean
        names = [mainsh mainout mainerr matin matout];
        if opt.job.use_dummy
            names = [names {dummy_sh dummy_out dummy_err}];
        end
        for i=1:numel(names)
            if exist(fullfile(opt.client.folder, names{i}), 'file')
                delete(fullfile(opt.client.folder, names{i}));
            end
        end
    end
    
end

% -------------------------------------------------------------------------
%   Estimate memory usage
% -------------------------------------------------------------------------

function opt = estimate_mem(opt,N)               
    sd = opt.job.sd;
    
    if opt.job.batch                
        jobid = opt.job.id{1};
        omem  = opt.job.mem{1};

        cmd = '';
        for i=1:numel(opt.client.source)
            cmd = [cmd 'source ' opt.client.source{i} ' >/dev/null 2>&1 ; '];
        end
        cmd = [cmd opt.ssh.bin ' ' opt.ssh.opt ' ' opt.server.login '@' opt.server.ip ' "'];
        for i=1:numel(opt.server.source)
            cmd = [cmd 'source ' opt.server.source{i} ' >/dev/null 2>&1 ; '];
        end
        cmd = [cmd opt.sched.acct ' '];
        cmd = [cmd ' -j ' num2str(jobid) ' | grep maxvmem"'];
          
        [status,result] = system(cmd);   

        if status==0
            jobs = strsplit(result,'G');
            S    = numel(jobs) - 1; 

            a = zeros(1,S);
            for s=1:S
                job  = jobs{s};                      
                job  = strsplit(job,' '); 
                a(s) = str2double(job{2});
            end

            a   = (1 + sd)*max(a);
            mem = ceil(a * 10)/10; % Ceil to one decimal place

            opt.job.mem{1} = [num2str(mem) 'G'];  
        else
            opt.job.mem{1} = omem; 
        end
        
        if opt.verbose
            fprintf('New memory usage is %s (old memory was %s)\n',opt.job.mem{1},omem);
        end
    else        
        N = numel(opt.job.id);
        for n=1:N
            jobid = opt.job.id{n};
            omem  = opt.job.mem{n};

            cmd = '';
            for i=1:numel(opt.client.source)
                cmd = [cmd 'source ' opt.client.source{i} ' >/dev/null 2>&1 ; '];
            end
            cmd = [cmd opt.ssh.bin ' ' opt.ssh.opt ' ' opt.server.login '@' opt.server.ip ' "'];
            for i=1:numel(opt.server.source)
                cmd = [cmd 'source ' opt.server.source{i} ' >/dev/null 2>&1 ; '];
            end
            cmd = [cmd opt.sched.acct ' '];
            cmd = [cmd ' -j ' num2str(jobid) ' | grep maxvmem"'];

            [status,result] = system(cmd);   

            if status==0
                job = strsplit(result,'G');    
                job = strsplit(job{1},' '); 
                a   = str2double(job{2});
                a   = (1 + sd)*a;
                mem = ceil(a * 10)/10; % Ceil to one decimal place

                opt.job.mem{n} = [num2str(mem) 'G']; 
            else
                opt.job.mem{n} = omem; 
            end
            
            if opt.verbose
                fprintf('New memory usage is %s (old memory was %s)\n',...
                    opt.job.mem{n},omem);
            end
        end
    end
end

% -------------------------------------------------------------------------
%  Create shell script to execute dummy job
% -------------------------------------------------------------------------

function [nam,sh,out,err] = create_dummy_job(opt)
    uid = char(java.util.UUID.randomUUID()); 
    nam = ['dummy_' uid];
    sh  = ['dummy_' uid '.sh'];        
    out = ['dummy_cout_' uid '.log'];  
    err = ['dummy_cerr_' uid '.log']; 
    
    bash_script = sprintf(['#!/bin/sh\n'...
                                 '\n'...
                                 '#$ -S /bin/sh\n'...
                                 '#$ -N ' nam '\n'...
                                 '#$ -o ' fullfile(opt.server.folder,out) '\n'...
                                 '#$ -e ' fullfile(opt.server.folder,err) '\n'...
                                 '#$ -j n \n'...
                                 '#$ -t 1-1 \n'...
                                 '\n'...
                                 'echo dummy\n']);
                             
    pth = fullfile(opt.client.folder,sh);
    
    fid = fopen(pth,'w');
    fprintf(fid,bash_script);
    fclose(fid);

    fileattrib(pth,'+x','u')
end

% -------------------------------------------------------------------------
%   Print stuff
% -------------------------------------------------------------------------

function fprintf_job(opt,N,t)            
    date = datestr(now,'mmmm dd, yyyy HH:MM:SS');
    if nargin<3        
        fprintf('\n----------------------------------------------\n')
        if opt.job.batch
            fprintf('%s | Batch job submitted to cluster (N = %i)\n',date,N)
        else
            fprintf('%s | Individual jobs submitted to cluster (N = %i)\n',date,N)
        end
    else
        fprintf('%s | Cluster processing finished (%.1f seconds)\n',date,t)
    end
end
