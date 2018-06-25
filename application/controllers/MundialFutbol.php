<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class MundialFutbol extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->database();
  }

  public function partidos_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
        
        $query = "SELECT 
                        a.*,
                        lc.nombre AS n_loc,
                        vi.nombre AS n_vi,
                        lc.bandera AS b_loc,
                        vi.bandera AS b_vi,
                        lc.grupo,
                        qn.gf AS pr_gf,
                        qn.gc AS pr_gc
                    FROM
                        mundial2018_partidos a
                            LEFT JOIN
                        mundial2018_equipos lc ON a.equipo = lc.id
                            LEFT JOIN
                        mundial2018_equipos vi ON a.rival = vi.id
                            LEFT JOIN
                        mundial2018_quiniela qn ON a.id = qn.id AND qn.asesor = ".$_GET['usid']."
                    WHERE
                        a.local = 1";
          
      if( $q = $this->db->query($query) ){    
        okResponse( 'Información Obtenida', 'data', $q->result_array(), $this );
      }else{
        errResponse('Error al obtener reporte personalizado', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
      }

    });

  }

  public function tablaQuiniela_get(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $qDep = "SELECT dep FROM dep_asesores WHERE Fecha=CURDATE() AND asesor=".$_GET['usid'];
        $qD = $this->db->query($qDep);
        $dep = $qD->row_array();

        if( $dep['dep'] == 29 ){
            $pdv = "AND dep = 29";
        }else{
            $pdv = "AND dep != 29";
        }
        
        $query = "SELECT 
                    qn.asesor, 
                    NOMBREASESOR(qn.asesor,1) as Nombre,
                    SUM(IF(finalizado=1 OR live = 1, IF(
                            (a.gf>a.gc AND qn.gf>qn.gc) OR
                            (a.gf<a.gc AND qn.gf<qn.gc) OR
                            (a.gf=a.gc AND qn.gf=qn.gc)
                        ,10,0) +
                        IF( a.gf = qn.gf , 15, 0 ) +
                        IF( a.gc = qn.gc , 15, 0 ) +
                        IF( a.gc = qn.gc AND a.gf = qn.gf, 20, 0 ),0)
                    ) as pts
                FROM
                    mundial2018_partidos a
                        RIGHT JOIN
                    mundial2018_quiniela qn ON a.id = qn.id 
                        LEFT JOIN 
                    dep_asesores c ON qn.asesor=c.asesor AND c.Fecha=CURDATE()
                WHERE
                    a.local = 1 $pdv 
                GROUP BY qn.asesor ORDER BY pts DESC, Nombre";
          
      if( $q = $this->db->query($query) ){    
        okResponse( 'Información Obtenida', 'data', $q->result_array(), $this );
      }else{
        errResponse('Error al obtener reporte personalizado', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
      }

    });

  }

  public function quiniela_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $data = $this->put();

        if($val = $this->db->select('IF( NOW() < Fecha, 1, 0) as flag', FALSE)->from('mundial2018_partidos')->where('id', $data['id'])->get() ){
            $validate = $val->row_array();
        }else{
            errResponse('Error al validar', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }

        if( $validate['flag'] == 0 ){
            errResponse('El partido ya finalizó. No es posible modificar tu pronóstico', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', false);
        }

        $ins = $this->db->set($data)
                ->set(array('asesor' => $_GET['usid']))
                ->get_compiled_insert('mundial2018_quiniela');
        
        $query = "$ins ON DUPLICATE KEY UPDATE gf=".$data['gf'].", gc=".$data['gc'];
          
      if( $q = $this->db->query($query) ){    
        okResponse( 'Información Guardada', 'data', true, $this );
      }else{
        errResponse('Error al guardar. Recuerda ingresar el marcador completo', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
      }

    });

  }

  public function resultSet_put(){

    $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $data = $this->put();

        $updateA = array( 'gf' => $data['gf'], 'gc' => $data['gc'], 'finalizado' => $data['ended'], 'live' => $data['live'], 'editable' => $data['editable'] );
        $whereA = array( 'equipo' => $data['l'], 'rival' => $data['v'] );
        $updateB = array( 'gf' => $data['gc'], 'gc' => $data['gf'], 'finalizado' => $data['ended'], 'live' => $data['live'], 'editable' => $data['editable'] );
        $whereB = array( 'equipo' => $data['v'], 'rival' => $data['l'] );

      if( $this->db->where($whereA)->set($updateA)->update('mundial2018_partidos') ){    
        if( $this->db->where($whereB)->set($updateB)->update('mundial2018_partidos') ){    
            okResponse( 'Información Guardada', 'data', true, $this );
          }else{
            errResponse('Error al obtener reporte personalizado', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
          }
      }else{
        errResponse('Error al obtener reporte personalizado', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
      }

    });

  }

  public function partidosRAW_get(){

    $fQ = $this->db->query("SELECT IF(HOUR(NOW()) > '04:30:00' AND HOUR(NOW()) < '16:00:00',1,0) as flag");
    $flag = $fQ->row_array();

    if( $flag['flag'] == 0 ){
        okResponse( 'Información Obtenida', 'data', array(), $this );
    }
    
    $query = "SELECT 
                    *
                FROM
                    mundial2018_partidos
                WHERE local=1 AND editable=1 AND Fecha BETWEEN CURDATE() AND ADDDATE(CURDATE(),1)";
        
    if( $q = $this->db->query($query) ){    
    okResponse( 'Información Obtenida', 'data', $q->result_array(), $this );
    }else{
    errResponse('Error al obtener reporte personalizado', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
    }

  }
 
}
