@Library('ratcheting')_
import ru.curs.ratcheting.utils

node {    
    def server = Artifactory.server 'ART'
    def rtMaven = Artifactory.newMavenBuild()
    def buildInfo
    def modules = ['celesta-sql', 'celesta-core', 'celesta-maven-plugin',
                    'celesta-system-services', 'celesta-java', 'celesta-jython', 'dbschemasync']
    def warningsMap = [:];
    
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

    stage ('Static analysis') {
        for (module in modules) {
            def checkStyleResultPath = findFiles(glob: module + "/target/checkstyle-result.xml")[0].path
            def findBugsXmlPath = findFiles(glob: module + "/target/findbugsXml.xml")[0].path
            def targetPath = checkStyleResultPath.substring(0, checkStyleResultPath.lastIndexOf("/"))

            def shScript = $/{
                            printf 'checkstyle: ' ;
                            xmllint --xpath 'count(/checkstyle/file/error)' ${checkStyleResultPath} ;
                            echo;
                            printf 'findbugs: ' ;
                            xmllint --xpath 'count(/BugCollection/BugInstance)' ${findBugsXmlPath} ;
                            echo;
                        } | sed -e 'N;s/count:\n/\n  count: /g' > ${targetPath}/warnings.yml/$
            sh shScript

            warnings = readYaml file: targetPath + '/warnings.yml'
            echo "${module} checkstyle warnings count: ${warnings.checkstyle}"
            echo "${module} findbugs warnings count: ${warnings.findbugs}"
            warningsMap.put(module, warnings)
        }
        writeYaml file: 'target/warnings.yml', data: warningsMap
    }
       
    stage ('Ratcheting') {
      def downloadSpec = """
         {"files": [
            {
              "pattern": "warn/celesta/*/warnings.yml",
              "build": "celesta :: dev/LATEST",
              "target": "previous.yml",
              "flat": "true"
            }
            ]
        }""" 
        server.download spec: downloadSpec
        def oldWarnings = readYaml file: 'previous.yml'
        if (!(new utils()).compareWarnings(oldWarnings, warningsMap)){
           error "Ratcheting failed, see messages above."
        }
    }
    
    if (env.BRANCH_NAME == 'dev') {
        stage ('Publish build info') {
            def uploadSpec = """
            {
             "files": [
                {
                  "pattern": "target/warnings.yml",
                  "target": "warn/celesta/${currentBuild.number}/warnings.yml"
                }
                ]
            }"""
            
            def buildInfo2 = server.upload spec: uploadSpec
            buildInfo.append(buildInfo2)
            server.publishBuildInfo buildInfo
        }
    }
}
