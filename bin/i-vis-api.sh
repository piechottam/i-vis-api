#!/bin/bash

export FLASK_APP="i_vis.api:create_api_app()"
export FLASK_ENV="development"
export I_VIS_CONF="/home/michael/git/i-vis/i-vis-api/conf/i-vis.conf"

flask $@
