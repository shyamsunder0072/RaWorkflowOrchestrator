from flask_appbuilder.security.views import AuthView
from airflow.www_rbac.decorators import action_logging
from flask_login import login_user, logout_user
from flask_appbuilder import expose
from flask import (
    redirect, request, Markup, Response, render_template,
    make_response, flash, jsonify, send_file, url_for, g)
from flask_appbuilder.security.forms import LoginForm_db
from airflow.models import Log
from airflow.utils.db import create_session
from flask_appbuilder._compat import as_unicode

class CoutureAuthView(AuthView):
    login_template = 'appbuilder/general/security/login_db.html'

    @expose('/login/', methods=['GET', 'POST'])
    def login(self):
        if g.user is not None and g.user.is_authenticated:
            return redirect(self.appbuilder.get_url_for_index)
        form = LoginForm_db()
        if form.validate_on_submit():
            user = self.appbuilder.sm.auth_user_db(form.username.data, form.password.data)
            if not user:
                flash(as_unicode(self.invalid_login_message), 'warning')
                return redirect(self.appbuilder.get_url_for_login)
            login_user(user, remember=False)
            with create_session() as session:
                log = Log(
                    event="login",
                    task_instance=None,
                    owner=user.username,
                    extra=None,
                    task_id=None,
                    dag_id=None,
                    source_ip=request.environ['REMOTE_ADDR'])

                session.add(log)
            return redirect(self.appbuilder.get_url_for_index)
        return self.render_template(self.login_template,
                                    title=self.title,
                                    form=form,
                                    appbuilder=self.appbuilder)
