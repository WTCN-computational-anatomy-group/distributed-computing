function varargout = distribute_translate(opt, varargin)
% FORMAT [out1, ...] = distribute_translate(trans, arg1, ...,)
%
% opt - Option structure with fields:
%     translation - Cell array of size 2xN with translation between client 
%                   and server paths [{client.folder server.folder}].
%                   Example:
%                       {'/home/me/'     '/mnt/users/me/' ;
%                        '/shared/data/' '/mnt/shared/data'}
%     restrict    - Restrict translation to either 'char' or 'file_array'
%                   objects. If empty, do not restrict.
% arg   - Argument that needs translation (string, file_array, struct, ...)

    for i=1:min(numel(varargin), nargout)
        varargout{i} = translate(opt, varargin{i});
    end
    
end

function obj = translate(opt, obj)
    
    if ischar(obj) && ~strcmpi(opt.restrict, 'file_array')
        for j=1:size(opt.translate, 1)
            obj = strrep(obj, opt.translate(j,1), opt.translate(j,2));
        end
        return
    end
    if isa(obj, 'file_array') && ~strcmpi(opt.restrict, 'char')
        obj.fname = translate(opt, obj.fname);
        return
    end
    if isa(obj, 'nifti')
        obj.dat = translate(opt, obj.dat);
        return
    end
    if isstruct(obj)
        fields = fieldnames(obj);
        for j=1:numel(fields)
            field = fields{j};
            obj.(field) = translate(opt, obj.(field));
        end
        return
    end
    if iscell(obj)
        for j=1:numel(obj)
            obj{j} = translate(opt, obj{j});
        end
        return
    end
    if isa(obj, 'matlab.ui.Figure')
        obj = [];
        return
    end

end