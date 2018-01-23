<?php
namespace Google\Cloud\Bigtable\src;

use Google\Cloud\Bigtable\Admin\V2\BigtableInstanceAdminClient;

class Bigtable
{
    public function connection($projectId, $options = [])
    {
        putenv('GOOGLE_APPLICATION_CREDENTIALS='. $options['credentials']);
        $BigtableInstanceAdminClient = new BigtableInstanceAdminClient();
        $formattedParent = $BigtableInstanceAdminClient->projectName($projectId);
		return $formattedParent;
    }
}

?>