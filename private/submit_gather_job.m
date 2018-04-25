function id = submit_gather_job(opt, server_dir, sh, dependency)

    % ---------------------------------------------------------------------
    % Build command
    cmd = '';
    for i=1:numel(opt.client.source)
        cmd = [cmd 'source ' opt.client.source{i} ' >/dev/null 2>&1 ; '];
    end
    cmd = [cmd opt.ssh.bin ' ' opt.ssh.opt ' ' opt.server.login '@' opt.server.ip ' "'];
    for i=1:numel(opt.server.source)
        cmd = [cmd 'source ' opt.server.source{i} ' >/dev/null 2>&1 ; '];
    end
    cmd = [cmd opt.sched.sub ' '];
    cmd = [cmd ...
            ' -l vf=0.1G -l h_vmem=0.1G' ...
            ' -hold_jid ' dependency ...
            ' -cwd '      fullfile(server_dir, sh) '"'];

    % ---------------------------------------------------------------------
    % Call command
    [status,result] = system(cmd);
    if status
        fprintf([result '\n'])
        error('status~=0 for gathering job on server!') 
    end

    % ---------------------------------------------------------------------
    % Get ID
    s  = regexp(result, '^\D*(?<id>\d+)', 'names'); % ID of array job on server    
    id = s.id;
end