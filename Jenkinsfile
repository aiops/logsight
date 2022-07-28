pipeline {
    agent any

    environment {
        DOCKER = credentials('dockerhub')
        DOCKER_REPO = "logsight/logsight"
    }

    stages {
        stage('Test') {
            agent {
                docker {
                    image 'python:3.8'
                }
            }
            steps {
                script {
                    env.RETRY_ATTEMPTS = 2
                    env.RETRY_TIMEOUT = 1
                }
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
                                sh """ 
                                    sonar-scanner -Dsonar.projectKey=aiops_logsight -Dsonar.branch.name=$BRANCH_NAME \
                                        -Dsonar.organization=logsight \
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
                        sh 'hadolint --format json Dockerfile | tee -a hadolint.json'
                    }
                    post {
                        always {
                            archiveArtifacts 'hadolint.json'
                            recordIssues(
                                tools: [hadoLint(pattern: "hadolint.json", id: "dockerfile")]
                            )
                        }
                    }
                }
            }
        }

    }
}