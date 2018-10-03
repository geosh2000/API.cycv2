<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Lists extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->helper('mailing');
    $this->load->database();
  }

  public function chanGroup_put(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
            
            $data        = $this->put();
            $filters     = $data['filters'];
            $searchField = $data['field'];
            
            $this->db->select("$searchField as id, $searchField as name")
                ->from('chanGroups')
                ->group_by($searchField)
                ->order_by($searchField);   
            
            foreach( $filters as $field => $info ){
                if( $field != $searchField ){
                    if(count($info['params'])>0){
                        $this->db->where_in( $info['name'], $info['params'] );
                    }
                }
            }
            
            if( $q = $this->db->get() ){
                
                okResponse( 'Info Obtenida', 'data', $q->result_array(), $this, 'filters', $filters);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
        jsonPrint( $result );
    }
    
    public function tipoRsva_put(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
            
            $data        = $this->put();
            $filters     = $data['filters'];
            $searchField = $data['field'];
            
            $this->db->select("$searchField as id, $searchField as name")
                ->from('config_tipoRsva')
                ->group_by($searchField)
                ->order_by($searchField);   
            
            foreach( $filters as $field => $info ){
                if( $field != $searchField ){
                    if(count($info['params'])>0){
                        $this->db->where_in( $info['name'], $info['params'] );
                    }
                }
            }
            
            if( $q = $this->db->get() ){
                
                okResponse( 'Info Obtenida', 'data', $q->result_array(), $this, 'filters', $filters);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
        jsonPrint( $result );
    }
    
    public function itemTypes_put(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
            
            $data        = $this->put();
            $filters     = $data['filters'];
            $searchField = $data['field'];
            
            $this->db->select("$searchField as id, $searchField as name")
                ->from('itemTypes')
                ->group_by($searchField)
                ->order_by($searchField);   
            
            foreach( $filters as $field => $info ){
                if( $field != $searchField ){
                    if(count($info['params'])>0){
                        $this->db->where_in( $info['name'], $info['params'] );
                    }
                }
            }
            
            if( $q = $this->db->get() ){
                
                okResponse( 'Info Obtenida', 'data', $q->result_array(), $this, 'filters', $filters);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
        jsonPrint( $result );
    }   
    
    public function branchId_put(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
            
            $data        = $this->put();
            $filters     = $data['filters'];
            $searchField = $data['field'] == 'branchId' ? 'branchid' : 'cityForListing';
            
            switch( $data['field'] ){
                case 'branchId':
                    $this->db->select("branchid as id, PDV as name");
                    break;
                case 'Localidad':
                    $this->db->select("cityForListing as id, cityForListing as name");
                    break;
            }
            
            $this->db->from('PDVs')
                ->group_by($searchField)
                ->order_by('PDV');   
            
            foreach( $filters as $field => $info ){
                if( $field != $searchField ){
                    if(count($info['params'])>0){
                        $this->db->where_in( $info['name'] == 'Localidad' ? 'cityForListing' : $info['name'] , $info['params'] );
                    }
                }
            }
            
            if( $q = $this->db->get() ){
                
                okResponse( 'Info Obtenida', 'data', $q->result_array(), $this, 'filters', $filters);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
        jsonPrint( $result );
    }

    public function pdvSuper_put(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
            
            $query = "SELECT 
                FINDSUPPDVDAY(id, CURDATE(), 0) AS id,
                FINDSUPPDVDAY(id, CURDATE(), 2) AS name
            FROM
                PDVs
            GROUP BY name
            HAVING name IS NOT NULL ORDER BY name";
            
            if( $q = $this->db->query($query) ){
                
                okResponse( 'Info Obtenida', 'data', $q->result_array(), $this, 'filters', null);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
        jsonPrint( $result );
    }

    public function listProfiles_get(){
        $this->db->select('id, profile_name as name')
            ->from('profilesDB')
            ->order_by('profile_name');

        if( $q = $this->db->get() ){
            okResponse( 'Info Obtenida', 'data', $q->result_array(), $this);
        }else{
            errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }
    }

    public function pdvList_get(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $this->db->select("a.*,
                CONCAT(displayNameShort, ' - ', cityForListing) AS displayNameList", FALSE)
                ->from('PDVs a')
                ->join('cat_zones b', 'a.branchZoneId = b.id', 'left')
                ->where('Activo',1);
            
            if( $q = $this->db->get() ){
                
                okResponse( 'Info Obtenida', 'data', $q->result_array(), $this, 'filters', null);
                
            }else{
                errResponse('Error en la base de datos', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;
        });
      
        jsonPrint( $result );
    }

}
