def set_options(opt):
    opt.tool_options('compiler_cxx')

def configure(conf):
    conf.check_tool('compiler_cxx')
    conf.check_tool('node_addon')

def build(bld):
    obj = bld.new_task_gen('cxx', 'shlib', 'node_addon')
    ## http://groups.google.com/group/nodejs/browse_thread/thread/af28b0857a60c7e6
    obj.cxxflags = ["-g", "-D_FILE_OFFSET_BITS=64", "-D_LARGEFILE_SOURCE", "-Wall"]
    obj.target = '_kyoto'
    obj.source = 'src/_kyoto.cc'
    obj.defines = "__STDC_LIMIT_MACROS"
    obj.lib = ["kyotocabinet"]
