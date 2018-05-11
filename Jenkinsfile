node {
    def server = Artifactory.server 'ART'
    def rtMaven = Artifactory.newMavenBuild()
    def buildInfo
    def modules = ['celesta-sql', 'celesta-core', 'celesta-maven-plugin',
                    'celesta-system-services', 'celesta-java', 'celesta-jython', 'dbschemasync']

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
            rtMaven.run pom: 'pom.xml', goals: 'clean install -P dev', buildInfo: buildInfo
        }
    } finally {
        junit '**/surefire-reports/**/*.xml'
        checkstyle pattern: '**/target/checkstyle-result.xml' //, canComputeNew: true, useDeltaValues: true, shouldDetectModules: true
        findbugs pattern: '**/target/findbugsXml.xml'
    }

        stage ('Static Analysis') {
            def warningsMap = [:];
            for (module in modules) {
                def checkStyleResultPath = findFiles(glob: module + "/target/checkstyle-result.xml")[0].path
                def findBugsXmlPath = findFiles(glob: module + "/target/findbugsXml.xml")[0].path
                def targetPath = checkStyleResultPath.substring(0, checkStyleResultPath.lastIndexOf("/"))

                def shScript = $/{
                                echo 'checkstyle: count:' ;
                                xmllint --xpath 'count(/checkstyle/file/error)' ${checkStyleResultPath} ;
                                echo;
                                echo 'findbugs: count:' ;
                                xmllint --xpath 'count(/BugCollection/BugInstance)' ${findBugsXmlPath} ;
                            } | sed -e 'N;s/count:\n/\n  count: /g' > ${targetPath}/warnings.yaml/$
                sh shScript

                warnings = readYaml file: targetPath + '/warnings.yaml'
                echo "${module} checkstyle warnings count: ${warnings.checkstyle.count}"
                echo "${module} findbugs warnings count: ${warnings.findbugs.count}"
                warningsMap.put(module, warnings)
            }
            writeYaml file: 'target/warnings.yaml', data: warningsMap
        }

    if (env.BRANCH_NAME == 'dev') {
        stage ('Publish build info') {
            server.publishBuildInfo buildInfo
        }
    }
}