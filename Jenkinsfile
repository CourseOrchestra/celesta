node {
    def server = Artifactory.server 'ART'
    def rtMaven = Artifactory.newMavenBuild()
    def buildInfo

    stage ('Clone') {
        checkout scm
    }

    stage ('Artifactory configuration') {
        rtMaven.tool = 'M3' 
        rtMaven.deployer releaseRepo: 'libs-release-local', snapshotRepo: 'libs-snapshot-local', server: server
        rtMaven.resolver releaseRepo: 'libs-release', snapshotRepo: 'libs-snapshot', server: server
        buildInfo = Artifactory.newBuildInfo()
        buildInfo.env.capture = true
    }
    
    stage ('Docker cleanup') {
        sh '''docker ps -a -q &> /dev/null
if [ $? != 0 ]; then
   docker rm $(docker ps -a -q)
fi'''
    }

    try{
        stage ('Exec Maven') {
            ansiColor('xterm') {
                rtMaven.run pom: 'pom.xml', goals: 'clean install -P dev', buildInfo: buildInfo
            }
        }
    } finally {
        junit '**/surefire-reports/**/*.xml'
    }
    
    stage ('Publish build info') {
        server.publishBuildInfo buildInfo
    }
}