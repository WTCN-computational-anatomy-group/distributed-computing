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
            opt = estimate_mem(opt);            
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
%   Estimate memory usage
% -------------------------------------------------------------------------

function opt = estimate_mem(opt)
    sd = opt.job.sd;
    
    % ---------------------------------------------------------------------
    % BATCH MODE
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
            result = regexp(result, 'maxvmem\W+(?<mem>\d+.?\d*)(?<unit>[KMGT]?)', 'names');
            N = numel(result) - 1;
            
            % Convert all in bytes
            a = zeros(1,N);
            for n=1:N
                a(n) = str2double(result(n).mem);
                switch lower(result(n).unit)
                    case 'k'
                        a(n) = a(n) * 1024;
                    case 'm'
                        a(n) = a(n) * (1024^2);
                    case 'g'
                        a(n) = a(n) * (1024^3);
                    case 't'
                        a(n) = a(n) * (1024^4);
                end
            end
            % Compute maximum value
            [mx, i] = max(a);
            a = (1 + sd)*mx;
            % Use biggest unit
            units   = {result.unit};
            mxunit  = units{i(1)};
            switch lower(mxunit)
                case 'k'
                    a = a ./ 1024;
                case 'm'
                    a = a ./ (1024^2);
                case 'g'
                    a = a ./ (1024^3);
                case 't'
                    a = a ./ (1024^4);
            end
            mem = ceil(a * 10)/10; % Ceil to one decimal place
            opt.job.mem{1} = [num2str(mem) mxunit];  
        else
            opt.job.mem{1} = omem; 
        end
        
        if opt.verbose
            fprintf('New memory usage is %s (old memory was %s)\n',...
                    opt.job.mem{1},omem);
        end
        
        
    % ---------------------------------------------------------------------
    % BATCH MODE
    else        
        N = numel(opt.job.id);

        cmd = '';
        for i=1:numel(opt.client.source)
            cmd = [cmd 'source ' opt.client.source{i} ' >/dev/null 2>&1 ; '];
        end
        cmd = [cmd opt.ssh.bin ' ' opt.ssh.opt ' ' opt.server.login '@' opt.server.ip ' "'];
        for i=1:numel(opt.server.source)
            cmd = [cmd 'source ' opt.server.source{i} ' >/dev/null 2>&1 ; '];
        end
        for n=1:N
            jobid = opt.job.id{n};
            cmd = [cmd opt.sched.acct ' -j ' jobid ' | grep maxvmem ; '];
        end
        cmd = [cmd '"'];

        [status,result] = system(cmd);   

        if status==0
            result = regexp(result, 'maxvmem\W+(?<mem>\d+.?\d*)(?<unit>[KMGT]?)', 'names');
            for n=1:N
                omem = opt.job.mem{n};
                a   = str2double(result(n).mem);
                a   = (1 + sd)*a;
                mem = ceil(a * 10)/10; % Ceil to one decimal place
                opt.job.mem{n} = [num2str(mem) result(n).unit]; 
            
                if opt.verbose
                    fprintf('New memory usage is %s (old memory was %s)\n',...
                            opt.job.mem{n},omem);
                end
            end
        end
    end
end
