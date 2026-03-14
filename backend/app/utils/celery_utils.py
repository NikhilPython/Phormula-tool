from celery import Celery, Task

class FlaskTask(Task):
    _flask_app = None

    def __call__(self, *args, **kwargs):
        if self._flask_app is None:
            return self.run(*args, **kwargs)

        with self._flask_app.app_context():
            return self.run(*args, **kwargs)

def celery_init_app(app):
    FlaskTask._flask_app = app

    celery_app = Celery(app.import_name, task_cls=FlaskTask)
    celery_app.config_from_object(app.config["CELERY"])
    celery_app.set_default()
    app.extensions["celery"] = celery_app
    return celery_app