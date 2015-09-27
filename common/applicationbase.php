<?php  
class applicationbase {

	public function __toString() {
		$retval = '';
		$object_vars = get_object_vars ($this);
		while ( list ($key, $value) = each ($object_vars) ) {	//nice hash iterator code
			$print_val = $value;
			if (isset($print_val)) {
				$print_val = is_array ($value) ? sizeof($value) : $value;
			
			}
			else {
				$print_val = 'NULL';
			}
			$retval .= $key . '>> ' . $print_val . "\n";
		}
		return $retval;
	}
}








  


?>
