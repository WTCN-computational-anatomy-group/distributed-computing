function track_jobs(client_dir, main_end, gather_end, verbose)

    N           = numel(main_end);
    finished    = zeros(1,N);
    nb_finished = 0;
    use_gather  = ~isempty(gather_end);
    
    if verbose
        start_track = uihelper('begin');
    end
    while 1
        % Do not try to often
        pause(10);
        
        % Track individual jobs
        for i=1:N
            finished(i) = finished(i) || exist(fullfile(client_dir, main_end{i}), 'file');
        end
        prev_finished = nb_finished;
        nb_finished   = sum(finished);
        if verbose
            uihelper('incr', nb_finished, prev_finished, N, 50);
        end
        
        % Track gathering job
        if use_gather
            if nb_finished == N && ...
               exist(fullfile(client_dir, gather_end{i}), 'file')                            
                break
            end
        elseif nb_finished == N
            break;
        end
    end
    if verbose
        uihelper('end', N, start_track);
    end
    
end