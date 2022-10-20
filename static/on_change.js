$(function(){
        var $select1 = $( '#category_id' ),
		$select2 = $( '#movie_id' ),
        $options = $select2.find( 'option' );    
        $select1.on( 'change', function() {
	    $select2.html( $options.filter( '[name="' + this.value + '"]' ) );
        } ).trigger( 'change' );
    })