<?php

use Google\Cloud\Bigtable\Admin\V2\BigtableInstanceAdminClient;
use Google\Bigtable\Admin\V2\Instance;
use Google\Bigtable\Admin\V2\Cluster;
use Google\Protobuf\Internal\MapField;
use Google\Protobuf\Internal\GPBType;

/**
* 
*/
class BigtableInstance 
{
	/**
	* Constructor
	* @param array $options {
    *                       Options for configuring the service API wrapper.
	* 		@type string $projectId		The unique name of the project
	* 		@type string $instanceId 	The unique name of the instance
	*/
	function __construct($options)
	{
		$this->projectId = $options['projectId'];//"grass-clump-479";//'grape-spaceship-123';
		$this->instanceId = $options['instanceId'];//"node-perf";//'grapebigtable123';
		$this->previousCell = array();
	}

	/**
     * Formats a string containing the fully-qualified path to represent
     * a project resource.
     *
     * @param string $project
     *
     * @return string The formatted project resource.
     * @experimental
     */
	public function projectName($projectId = '')
	{
		if(!$projectId){
			$projectId = $this->projectId;	
		}
		$formattedParent = BigtableInstanceAdminClient::projectName($projectId);
		return $formattedParent;
	}

	/**
     * Formats a string containing the fully-qualified path to represent
     * a instance resource.
     *
     * @param string $project Optional
     * @param string $instance Optional
     *
     * @return string The formatted instance resource.
     * @experimental
     */
	public function instanceName($projectId = '', $instanceId = '')
	{
		if(!$projectId){
			$projectId = $this->projectId;	
		}
		if(!$instanceId){
			$instanceId = $this->instanceId;
		}
		$formattedParent = BigtableInstanceAdminClient::instanceName($projectId, $instanceId);
		return $formattedParent;
	}

	/**
     * Create an instance within a project.
     *
     * @param string   $parent       The unique name of the project in which to create the new instance.
     *                               Values are of the form `projects/<project>`.
     *                          
     * @param string   $instanceId   The ID to be used when referring to the new instance within its project,
     *                               e.g., just `myinstance` rather than
     *                               `projects/myproject/instances/myinstance`.
     *
     * @param string   $clusterId    cluseter id
     *
     * @param array    $optionalArgs {
     *                               Optional.
     *
     * @return \Google\GAX\OperationResponse
	 *
     * @throws \Google\GAX\ApiException if the remote call fails
     * @experimental
     */
	public function createInstace($parent, $instanceId, $clusterId, $optionalArgs = [])
	{
		try 
		{
	   		$BigtableInstanceAdminClient = new BigtableInstanceAdminClient();
			$instance = new Instance();

			$instance->setDisplayName($instanceId);
			$instance->setType(2);

			$clusters = new Cluster();
			$clusters->setName($clusterId);
			$clusters->setDefaultStorageType(2);
			$clusters->setLocation($parent."/locations/us-central1-c");
			$arr = new MapField(GPBType::STRING,GPBType::MESSAGE, Cluster::class);
			$arr[$clusterId] = $clusters;

			$response = $BigtableInstanceAdminClient->createInstance($parent, $instanceId, $instance, $arr, []);
			return $response;
	    }
	    finally {
	    	$BigtableInstanceAdminClient->close();
	    }
	}

	/**
     * Lists information about instances in a project.
     *
     * @param string $parent       The unique name of the project for which a list of instances is requested.
     *                             Values are of the form `projects/<project>`.
     * @param array  $optionalArgs {
     *                             Optional.
     *
     * @return array 			  List of instances
     *
     * @throws \Google\GAX\ApiException if the remote call fails
     * @experimental
    */
	public function listInstace($parent, $optionalArgs = [])
	{
		try 
		{
	   		$BigtableInstanceAdminClient = new BigtableInstanceAdminClient();
			$response = $BigtableInstanceAdminClient->listInstances($parent, []);
			$result=$response->getInstances();
			$instances = array();
			foreach ($result as $instance) {
				$instances[] = $instance->getName();
			}
			return $instances;
	    }
	    finally {
	    	$BigtableInstanceAdminClient->close();
	    }
	}

	/**
     * Delete an instance from a project.
     *
     * @param string $name         The unique name of the instance to be deleted.
     *                             Values are of the form `projects/<project>/instances/<instance>`.
     * @param array  $optionalArgs {
     *                             Optional.
     *
     * @throws \Google\GAX\ApiException if the remote call fails
     * @experimental
     */
	public function deleteInstance($name, $optionalArgs = [])
	{
		try 
		{
	   		$BigtableInstanceAdminClient = new BigtableInstanceAdminClient();
			$response = $BigtableInstanceAdminClient->deleteInstance($name, $optionalArgs);
			return $response;
	    }
	    finally {
	    	$BigtableInstanceAdminClient->close();
	    }
	}
}
?>