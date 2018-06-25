<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Aleatoriedad extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->database();
  }

  public function getData_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

        $result['calls'] = array();
        $result['cases'] = array();

        if( $data['type'] ){

            if($data['criteria'] == 6){
                $boA = $this->db->query("SELECT bo_area_id as id FROM bo_areas WHERE bo_area_id<100");
                $boR = $boA->result_array();
                $areas = array();
                foreach($boR as $index => $info){
                    array_push($areas, $info['id']);
                }

                $result['cases'] = $this->casesArray($data, $areas);
            }

            $result['calls'] = $this->callsArray($data);
        }else{

            $this->db->select('asesor')
                ->select("GETIDASESOR(FINDSUPDAY(asesor, '".$data['dates']['inicio']."'), 2) AS SUP")
                ->from("dep_asesores")
                ->where("Fecha", $data['dates']['inicio'])
                ->where("hc_udn IS NOT NULL ", NULL, FALSE )
                ->having("SUP", $data['criteria']);

            if( $q = $this->db->get() ){

                $as = $q->result_array();
                $asesores = array();
                foreach($as as $index => $info){
                    array_push($asesores, $info['asesor']);
                }

                $result['calls'] = $this->callsArray($data, $asesores);
                $result['cases'] = $this->casesArray($data, $asesores);


            }else{
                errResponse('Error al obtener data de asesores del Supervisor', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }
        }

        okResponse( 'Información Obtenida', 'data', array( 'calls' => $result['calls'], 'cases' => $result['cases'] ), $this );


          return true;

        });

        jsonPrint( $result );

      }
    

    
    private function callsArray( $data, $asesor = array() ){

        $this->db->select('ac_id as id, asesor, Fecha, Hora, Llamante, a.Cola, AsteriskID, Duracion_Real, Desconexion, Skill')
            ->select("NOMBREASESOR(asesor,1) as nombre", FALSE)
            ->from("t_Answered_Calls a")
            ->join("Cola_Skill b", "a.Cola=b.Cola", "left")
            ->where("Fecha BETWEEN '".$data['dates']['inicio']."' AND '".$data['dates']['fin']."'")
            ->where("Answered", 1);            

        if( $data['type'] ){
            $this->db->order_by("RAND()", FALSE)
                ->having("Skill", $data['criteria'])
                ->limit($data['q']);

            if( $q = $this->db->get() ){
                return $q->result_array();
            }else{
                errResponse('Error al obtener data de llamadas', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

        }else{

            $this->db->where_in('asesor', $asesor);
            $query = $this->db->get_compiled_select();
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS callsArray");
            $this->db->query("CREATE TEMPORARY TABLE callsArray $query");

            $tmpData = array();

            foreach($asesor as $index => $info){
                if( $qTmp = $this->db->query("SELECT * FROM callsArray WHERE asesor=$info ORDER BY RAND() LIMIT ".$data['q']) ){
                    $tmpArr = $qTmp->result_array();
                    foreach($tmpArr as $tmpInd => $tmpInfo){
                        array_push($tmpData, $tmpInfo);
                    }
                }else{
                    errResponse('Error al obtener data de llamadas', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
                }
            }

            return $tmpData;
        }



    }
    
    private function casesArray( $data, $asesor = array() ){
        
        $this->db->select('id, asesor, CAST(date_created as DATE) as Fecha, CAST(date_created as TIME) as Hora, em, localizador, b.area, a.area as areaId')
            ->select("NOMBREASESOR(asesor,1) as nombre", FALSE)
            ->from("bo_tipificacion a")
            ->join("bo_areas b", "a.area=b.bo_area_id", "left")
            ->where("date_created >= '".$data['dates']['inicio']." 00:00:00' AND date_created <= '".$data['dates']['fin']." 23:59:59'")
            ->where("status !=", 8);
        
        if( $data['type'] ){
            
            $this->db->where_in('a.area', $asesor);
            
            $query = $this->db->get_compiled_select();
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS casesArray");
            $this->db->query("CREATE TEMPORARY TABLE casesArray $query");

            $tmpData = array();

            foreach($asesor as $index => $info){
                if( $qTmp = $this->db->query("SELECT * FROM casesArray WHERE areaId=$info ORDER BY RAND() LIMIT ".$data['q']) ){
                    $tmpArr = $qTmp->result_array();
                    foreach($tmpArr as $tmpInd => $tmpInfo){
                        array_push($tmpData, $tmpInfo);
                    }
                }else{
                    errResponse('Error al obtener data de llamadas', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
                }
            }

            return $tmpData;
            
        }else{
            $this->db->where_in('asesor', $asesor);
            
            $query = $this->db->get_compiled_select();
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS casesArray");
            $this->db->query("CREATE TEMPORARY TABLE casesArray $query");

            $tmpData = array();

            foreach($asesor as $index => $info){
                if( $qTmp = $this->db->query($query." HAVING asesor=$info ORDER BY RAND() LIMIT ".$data['q']) ){
                    $tmpArr = $qTmp->result_array();
                    foreach($tmpArr as $tmpInd => $tmpInfo){
                        array_push($tmpData, $tmpInfo);
                    }
                }else{
                    errResponse('Error al obtener data de llamadas', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
                }
            }

            return $tmpData;
        }
        
        
        
    }

  

}
