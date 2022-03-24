pipeline {
    agent none

    stages {
        stage('Test') {
            agent {
                docker {
                    image 'python:3.7'
                }
            }
            steps {
                sh 'pip install -r requirements.txt'
                sh 'PYTHONPATH=$PWD/logsight py.test --junitxml test-report.xml --cov-report xml:coverage-report.xml --cov=logsight tests/'
                stash name: 'test-reports', includes: '*.xml' 
            }
            post {
                always {
                    junit 'test-report.xml'
                    archiveArtifacts '*-report.xml'
                }
            }
        }
        stage('Linting & SonarQube') {
            parallel {
                stage('SonarQube') {
                    agent {
                        docker {
                            image 'sonarsource/sonar-scanner-cli'
                        }
                    }
                    steps {
                        script {
                            unstash "test-reports"
                            withSonarQubeEnv('logsight-sonarqube') {
                                // comment in with SonarQube dev license to enable branch analysis
                                //sh """ 
                                //    sonar-scanner -Dsonar.projectKey=logsight -Dsonar.branch.name=$BRANCH_NAME \
                                //        -Dsonar.sources=logsight -Dsonar.tests=tests/. \
                                //        -Dsonar.inclusions="**/*.py" \
                                //        -Dsonar.python.coverage.reportPaths=coverage-report.xml \
                                //        -Dsonar.test.reportPath=test-report.xml
                                //"""
                                sh """
                                    sonar-scanner -Dsonar.projectKey=logsight
                                        -Dsonar.sources=logsight -Dsonar.tests=tests/. \
                                        -Dsonar.inclusions="**/*.py" \
                                        -Dsonar.python.coverage.reportPaths=coverage-report.xml \
                                        -Dsonar.test.reportPath=test-report.xml
                                """
                            }
                        }
                    }
                }
                stage ("Lint Dockerfile") {
                    agent {
                        docker {
                            image 'hadolint/hadolint:latest-debian'
                        }
                    }
                    steps {
                        sh 'hadolint --format checkstyle Dockerfile | tee -a hadolint.xml'
                    }
                    post {
                        always {
                            archiveArtifacts 'hadolint.xml'
                        }
                    }
                }
            }
        }
    }
}