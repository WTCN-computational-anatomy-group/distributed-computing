function out = newline
% Backward compatibility for newline
    if exist('newline','builtin')
        out = builtin('newline');
        return
    end

    out = char(10);    
end