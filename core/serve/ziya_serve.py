import tornado.ioloop
import tornado.web
import tornado.template
import os, signal
import tornado.process
import re
import json
from collections import namedtuple

AppInfo = namedtuple('AppInfo', 'name url')

proxymap = {}

template_loader = tornado.template.Loader(os.path.join(os.path.dirname(os.path.realpath(__file__)), "templates"))


class Bunch(object):
    def __init__(self, adict):
        self.__dict__.update(adict)

    def __repr__(self):
        return repr(self.__dict__)

    def __getattribute__(self, attr):
        try:
            return object.__getattribute__(self, attr)
        except AttributeError:
            return None

# TODO 文档readme
class DefaultProxyHandler(tornado.web.RequestHandler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print("INIT PROXYHANDLER ***")

    async def post(self):
        # TODO static port（version）， path dependency （ray）

        m = re.search(r"^/(?P<category>[api|app|model]*)/(?P<name>[^/]*)(?P<path>.*)$",
                      self.request.path)

        # TODO 添加info路由显示模型信息

        if m is None:
            print(f'{self.__class__.__name__} NO MATCH')
            self.set_status(404)
            self.finish()
            return

        m = Bunch(m.groupdict())
        # m.cwd = scan_folder_path
        m.url = self.request.path
        m.appname = f"{m.category}/{m.tag}/{m.name}"
        m.stopped = False
        m.proc = None
        m.handlers = None
        head = self.request.headers
        m.backend = head.get("ziya-backend", None)

        if m.appname in proxymap: m = proxymap[m.appname]

        from core.serve.model_handler import handler as model_handler

        # 获取 post 数据
        post_data = self.request.body_arguments
        post_data = {x: post_data.get(x)[0].decode("utf-8") for x in post_data.keys()}
        if not post_data:
            try:
                post_data = self.request.body.decode('utf-8')
                post_data = json.loads(post_data)
            except Exception as e:
                print(e)
                load_data_log = f'data in wrong format, can not json.loads! detail: {e}'
                #                 sys_logger.info(load_data_log)
                self.write(load_data_log)
                return

        response_data = model_handler(m, post_data)
        if m.category == 'model' and not response_data:
            self.set_status(404)
            self.finish()
            return

        self.write(response_data)


def make_app():
    return tornado.web.Application([
        (r"^/model/$", DefaultProxyHandler),
    ])


# @click.command()
# @click.option('--port', default=8888, help='port for the launchpad server')
# @click.argument('folder')
def run(port):
    app = make_app()

    async def shutdown():
        tornado.ioloop.IOLoop.current().stop()

        for (appname, appval) in proxymap.items():
            if not appval['stopped']:
                proc = appval['proc']
                if proc:
                    print('Stopping proc for app {}'.format(appname))
                    proc.proc.terminate()

    def exit_handler(sig, frame):
        tornado.ioloop.IOLoop.current().add_callback_from_signal(shutdown)

    signal.signal(signal.SIGTERM, exit_handler)
    signal.signal(signal.SIGINT, exit_handler)

    app.listen(port)
    print("Starting ziya launchpad on port {}".format(port))
    tornado.ioloop.IOLoop.current().start()


if __name__ == '__main__':
    run("9999")
