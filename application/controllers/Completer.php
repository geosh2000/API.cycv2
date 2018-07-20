<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Completer extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->database();
  }

  public function searchData_get(){
      
      $special = array('á', 'é', 'í', 'ó', 'ú', 'ñ');
      $replace = array('a', 'e', 'i', 'o', 'u', 'n');
      
      $viewAll  = $this->uri->segment(3);
      $udn      = $this->uri->segment(4);
      $area     = $this->uri->segment(5);
      $dep      = $this->uri->segment(6);
      $puesto   = $this->uri->segment(7);
      $field    = $this->uri->segment(8);
      $active   = $this->uri->segment(9);
      $term     = $this->uri->segment(10);
      
      if( trim($term) == "" ){
          $this->response("");
      }
      
      
      $search = explode("%20", str_replace('.', ' ', $term));
      
      
      $this->codigosName();
      
      $this->db->select("a.id as asesor, Nombre, CONCAT('(',IF(Egreso>CURDATE(),'','INACTIVO - '),IF(c.pcrc IS NULL, 'Otro', c.pcrc),' - ',puesto_name,')') as dep, IF(Egreso>CURDATE(),1,0) as Activo", FALSE)
          ->select("REPLACE( REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LOWER($field),'ñ','n'),'ú','u'),'ó','o'),'í','i'),'é','e'),'á','a') as searchTerm")
          ->from('Asesores a')
          ->join('dep_asesores b', 'a.id=b.asesor AND b.Fecha=CURDATE()', 'left', FALSE)
          ->join("codeNames c", "b.hc_puesto = c.id", "left")
          ->order_by("Nombre");
      
      // Active Filter
      switch( $active ){
          case 0:
          case 1:
              $this->db->where("a.Egreso >","CURDATE()", FALSE);
              break;
      }

      
      // Term Filter
      foreach($search as $i => $info){
          if( $info != "" ){
            $find = str_replace($special, $replace, strtolower($info));
            $this->db->like("REPLACE( REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LOWER($field),'ñ','n'),'ú','u'),'ó','o'),'í','i'),'é','e'),'á','a')", $find);
          }
      }
      
      // Limit Results

      // UDN Filter
      if( $udn != 0 ){
          $this->db->where_in('udn_id', explode("-", $udn));
      }          

      // area Filter
      if( $area != 0 ){
          $this->db->where_in('area_id', explode("-", $area));
      }          

      // departamento Filter
      if( $dep != 0 ){
          $this->db->where_in('departamento_id', explode("-", $dep));
      }          

      // puesto Filter
      if( $puesto != 0 ){
          $this->db->where_in('puesto_id', explode("-", $puesto));
      }

      
      if( $completer = $this->db->get() ){
          $this->response( $completer->result_array() );
      }else{
          errResponse('Error al obtener coincidencias', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
      }

    
      
  }
    
  public function codigosName(){
      
      $codeQuery = $this->db->select("a.id,
                        e.nombre as udn_name,
                        c.nombre as area_name,
                        b.nombre as departamento_name,
                        a.nombre as puesto_name,
                        d.Departamento as pcrc,
                        e.id as udn_id,
                        c.id as area_id,
                        b.id as departamento_id,
                        a.id as puesto_id")
          ->from("hc_codigos_Puesto a")
          ->join("hc_codigos_Departamento b", "a.departamento = b.id", "left")
          ->join("hc_codigos_Areas c", "b.area = c.id", "left")
          ->join("PCRCs d", "b.pcrc = d.id", "left")
          ->join("hc_codigos_UnidadDeNegocio e", "c.unidadDeNegocio = e.id", "left")
          ->get_compiled_select();
      
      $this->db->query("CREATE TEMPORARY TABLE codeNames $codeQuery");
      
  }
    
    
    
    
    
}