<?php
defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');

class Rrhh extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->database();
  }

  public function form_get(){
    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $this->db->where('pais', 'MX')
                ->from('form_eval_desemp')
                ->order_by('position');

        if( $q = $this->db->get() ){
            okResponse( 'Data obtenida', 'data', $q->result_array(), $this );
        }else{
            errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

    });
  }

  public function operaciones_get(){
    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        if( $q = $this->db->from('hc_operacion')->get() ){
            okResponse( 'Data obtenida', 'data', $q->result_array(), $this );
        }else{
            errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

    });
  }

  public function evaluaciones_put(){
    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $filters = $this->put();

      $this->db->select("a.asesor,
                          NOMBREASESOR(a.asesor, 2) AS Nombre,
                          IF(dep = 29,
                              FINDSUPERDAYPDV(CURDATE(), oficina, 2),
                              FINDSUPERDAYCC(CURDATE(), a.asesor, 2)) AS Supervisor,
                          a.id AS contrato,
                          NOMBREDEP(dep) AS Departamento,
                          fin,
                          c.Operacion,
                          e.id AS evaluacion,
                          IF(vacante IS NULL, 0, 1) as Activo,
                          IF(fin BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 20 DAY)
                                  AND activo = 1
                                  AND deleted = 0
                                  AND e.id IS NULL,
                              IF(vacante IS NULL, 0, 1),
                              0) AS createNew, e.date_created as FechaEval, e.status", FALSE)
                ->from('asesores_contratos a')
                ->join('dep_asesores b','a.asesor = b.asesor
                      AND CURDATE() = b.Fecha','left')
                ->join('hc_operacion c','b.operacion = c.id','left')
                ->join('asesores_evaluacionD e','a.asesor = e.asesor
                      AND a.id = e.contrato','left')
                ->where('tipo', 1)
                // ->having('createNew',1)
                // ->or_having('evaluacion IS NOT NULL')
                ->order_by('Nombre, fin DESC');
        
        if( isset($filters['asesor']) ){
          $this->db->where('a.asesor', $filters['asesor']);
        }
        
        if( isset($filters['operacion']) ){
          $this->db->where('b.operacion', $filters['operacion']);
        }
        
        if( isset($filters['inicio']) ){
          $this->db->where("fin BETWEEN '".$filters['inicio']."' AND '".$filters['fin']."'", "", FALSE);
        }


        if( $q = $this->db->get() ){
            okResponse( 'Data obtenida', 'data', $q->result_array(), $this );
        }else{
            errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

    });
  }

  public function saveEval_put(){
    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $this->db->set($data['form'])
                ->set($data['keys']);
    
      switch( $data['form']['status'] ){
          case 1:
          case 2:
            $this->db->set('created_by', $_GET['usid']);
            break;
          case 3:
          case 4:
            $this->db->set('manager_date', 'NOW()', FALSE);
            $this->db->set('manager', $_GET['usid']);
            break;
          case 5:
            $this->db->set('review_date', 'NOW()', FALSE);
            $this->db->set('reviewed_by', $_GET['usid']);
            break;
          case 6:
            $this->db->set('accepted_date', 'NOW()', FALSE);
            $this->db->set('asesor_accept_status', $data['keys']['asesor']);
            break;
      }

      $insert = $this->db->get_compiled_insert('asesores_evaluacionD');

      $update = "";
      foreach($data['form'] as $index => $info){
          $update .= "$index=VALUES($index),";  
      }

      switch( $data['form']['status'] ){
        case 1:
        case 2:
          $update .= 'created_by=VALUES(created_by),';
          break;
        case 3:
        case 4:
          $update .= 'manager_date=VALUES(manager_date), manager=VALUES(manager),';
          break;
        case 5:
          $update .= 'review_date=VALUES(review_date), reviewed_by=VALUES(reviewed_by),';
          break;
        case 6:
          $update .= 'accepted_date=VALUES(accepted_date), asesor_accept_status=VALUES(asesor_accept_status),';
          break;
    }

      $update = substr($update,0,-1);

      $query = $insert." ON DUPLICATE KEY UPDATE $update";
        
        if( $q = $this->db->query( $query ) ){
            okResponse( 'Registro guardado', 'data', true, $this );
        }else{
            errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

    });
  }

  public function getEval_put(){
    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

      $data = $this->put();

      $this->db->select("*, NOMBREASESOR(asesor,6) AS usuario")
                ->select("NOMBREASESOR(created_by,1) as sup, NOMBREASESOR(asesor_accept_status,1) as agent, NOMBREASESOR(manager,1) as manager, NOMBREASESOR(reviewed_by,1) as review", FALSE)
                ->from('asesores_evaluacionD')
                ->where('asesor', $data['asesor'])
                ->where('contrato', $data['contrato']);
        
        if( $q = $this->db->get( ) ){
            okResponse( 'Registro guardado', 'data', $q->row_array(), $this );
        }else{
            errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

    });
  }


}