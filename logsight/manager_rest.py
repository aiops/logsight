import os
from flask import Flask, jsonify
from flask import request

from config import global_vars
from logsight_classes.data_class import AppConfig
from run import parse_arguments, get_config, logger, create_manager
from modules.continuous_verification.jorge import ContinuousVerification
from utils.fs import verify_file_ext

app = Flask(__name__, template_folder="./")


@app.route('/api/v1/compute_log_compare', methods=['GET'])
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


@app.route('/api/v1/applications', methods=['POST'])
def create_app():
    data = request.json
    app_config = AppConfig(application_id=request.json.get("id"),
                           application_name=request.json.get("name"),
                           private_key=request.json.get("key"))
    return jsonify(manager.create_application(app_config))


@app.route("/api/v1/applications/<application_id>", methods=["DELETE"])
def delete_app(application_id):
    return jsonify(manager.delete_application(application_id))


if __name__ == '__main__':
    from waitress import serve

    args = parse_arguments()
    config = get_config(args)

    connection_conf_file = verify_file_ext(args['cconf'], ".json")
    cv_module = ContinuousVerification(connection_conf_file)

    with open(os.path.join(global_vars.CONFIG_PATH, 'banner.txt'), 'r') as f:
        logger.info(f.read())
    manager = create_manager(config)
    manager.setup()
    logger.info("Running manager.")
    app.run(debug=True, host='0.0.0.0', port=5554)
