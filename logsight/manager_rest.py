import json
import os
from multiprocessing import Process
from flask import Flask, jsonify, render_template
from flask import request

from config import global_vars
from config.global_vars import CONFIG_PATH
from run import parse_arguments, get_config, logger, create_manager
from modules.continuous_verification.jorge import ContinuousVerification
from utils.fs import verify_file_ext

app = Flask(__name__,template_folder="./")

#example = json.load(open('testfile.json'))
@app.route('/api/compute_log_compare', methods=['GET'])
def get_tasks():
    args = request.args
    application_id = args.get("applicationName")
    baseline_tag_id = args.get("baselineTag")
    compare_tag_id = args.get("compareTag")
    private_key = args.get("privateKey")
    result = cv_module.run_verification(application_id=application_id,
                                        private_key=private_key,
                                        baseline_tag_id=baseline_tag_id,
                                        compare_tag_id=compare_tag_id)

    return jsonify(result)

@app.route('/api/test', methods=['GET'])
def get_tasks_html():
    args = request.args
    application_id = args.get("applicationId")
    baseline_tag_id = args.get("baselineTagId")
    compare_tag_id = args.get("compareTagId")
    private_key = args.get("privateKey")
    # result = cv_module.run_verification(application_id=application_id,
    #                                     private_key=private_key,
    #                                     baseline_tag_id=baseline_tag_id,
    #                                     compare_tag_id=compare_tag_id)
    return render_template('html.jinja')

@app.route('/api/applications/create')
def create_app():
    pass
if __name__ == '__main__':
    args = parse_arguments()
    config = get_config(args)

    connection_conf_file = verify_file_ext(args['cconf'], ".json")
    cv_module = ContinuousVerification(connection_conf_file)

    with open(os.path.join(global_vars.CONFIG_PATH, 'banner.txt'), 'r') as f:
        logger.info(f.read())
    # manager = create_manager(config)
    # manager.setup()
    logger.info("Running manager.")
    # p = Process(target=manager.run)
    # p.daemon = True
    # p.start()
    app.run(debug=True, host='0.0.0.0', port=5554)
