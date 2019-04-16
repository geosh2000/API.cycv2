<?php
defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');

class Bonos extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->database();
  }
    

    public function buildBonos_get(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $month = $this->uri->segment(3);
            $year = $this->uri->segment(4);
            
            $data = $this->buildBonos( $month, $year );    
                
                $metas = $this->db->select("NOMBREDEP(skill) as NameDep", FALSE)
                    ->select('a.*')
                    ->from('metas a')
                    ->where(array('mes' => $month, 'anio' => $year))
                    ->get();
                
                okResponse('Parámetros obtenidos', 'data', array('resultado' => $data['data'], 'detalle' => $data['detalle']), $this, 'params', array('metas' => $metas->result_array(), 'params' => $data['params'], 'deps' => $data['deps'], 'searchedParams' => array('mes' => $month, 'anio' => $year)));

        }); 

    }
    
    private function compare($a, $b, $oper){
        switch($oper){
            case "=":
                return $a = $b ? true : false;
            case "=":
                return $a != $b ? true : false;
            case ">=":
                return $a >= $b ? true : false;
            case "<=":
                return $a <= $b ? true : false;
            case ">":
                return $a > $b ? true : false;
            case "<":
                return $a < $b ? true : false;
            
        }
    }
    
    private function buildBonos( $month, $year ){
        
        $params = $this->db->from('asesores_bonos_parametros')
                    ->where(array('b_month' => $month, 'b_year' => $year))
                    ->get();
            
            foreach( $params->result_array() as $ind => $info ){
                $p[$info['b_skill']][$info['b_puesto']] = $info;
            }
            
            if( $res = $this->db->select('a.*, review, status, review_notes')
                    ->select("NOMBREPUESTO(puesto) as NamePuesto, NOMBREDEP(dep) as NameDep", FALSE)
                    ->select("NOMBREASESOR(approber,1) as reviewer, b.Last_Update as lastReview", FALSE)
                    ->from('asesores_bonos a')
                    ->join('asesores_bonos_aprobacion b', 'a.asesor=b.asesor AND a.mes=b.mes AND a.anio = b.anio', 'left')
                    ->where(array( 'a.mes' => $month, 'a.anio' => $year ))
                    ->where('dep IS NOT ', 'NULL', FALSE)
                    ->where('puesto IS NOT ', 'NULL', FALSE)
                    ->order_by('NameDep,NamePuesto,Nombre')
                    ->get() ){
                
            foreach( $res->result_array() as $index => $info ){

                        $data[$info['dep']][$info['puesto']][$info['asesor']]['aplica'] = $info['aplica'];

                        for( $i = 1; $i <= 4; $i++ ){

                            if( !isset($p[$info['dep']][$info['puesto']]) ){ break;}

                            $data[$info['dep']][$info['puesto']][$info['asesor']]['detalle'] = $info;
                            $data[$info['dep']][$info['puesto']][$info['asesor']]['aprobacion'] = array( 'status' => $info['status'], 'review' => $info['review'], 'comments' => $info['review_notes'], 'reviewer' => $info['reviewer'], 'lu' => $info['lastReview'] );
                            $tmpPar = $p[$info['dep']][$info['puesto']];
                            $monto = $tmpPar['monto'];

                            $data[$info['dep']][$info['puesto']]['montoBono'] = $monto / $info['diasLaborales']*$info['diasAsesor'];
                            $monto = $monto / $info['diasLaborales']*$info['diasAsesor'];
                            $data[$info['dep']][$info['puesto']]['depName'] = $info['NameDep'];
                            $data[$info['dep']][$info['puesto']]['puestoName'] = $info['NamePuesto'];

                            $deps[$info['dep']]=$info['NameDep'];

                            //Parameters
                            if( isset($tmpPar["par_$i"]) ){

                                if( $info[$tmpPar["par_$i"]] >= $tmpPar["cum_$i"] ){
                                    if( $tmpPar["tope_$i"] == 0 ){
                                        $data[$info['dep']][$info['puesto']][$info['asesor']]['bono'][$tmpPar["par_$i"]] = $info[$tmpPar["par_$i"]] * ( $tmpPar["perc_$i"] * $monto );
                                    }else{
                                        $data[$info['dep']][$info['puesto']][$info['asesor']]['bono'][$tmpPar["par_$i"]] = $info[$tmpPar["par_$i"]] <= 1 ? $info[$tmpPar["par_$i"]] * ( $tmpPar["perc_$i"] * $monto ) : ( $tmpPar["perc_$i"] * $monto );
                                    }
                                }else{
                                    $data[$info['dep']][$info['puesto']][$info['asesor']]['bono'][$tmpPar["par_$i"]] = 0;
                                }

                                if( $tmpPar["lock_$i"] != NULL ){
                                    if( strpos('|',$tmpPar["lock_$i"])>=0 ){
                                        $flag = FALSE;
                                        $locks = explode( '|', $tmpPar["lock_$i"] );
                                        foreach( $locks as $lind => $linf ){
                                            if( $data[$info['dep']][$info['puesto']][$info['asesor']]['bono'][$tmpPar["par_$linf"]] != 0 ){
                                                $flag = TRUE;
                                            }
                                        }

                                        if( !$flag ){
                                            $data[$info['dep']][$info['puesto']][$info['asesor']]['bono'][$tmpPar["par_$i"]] = 0;
                                        }
                                    }else{
                                        $locks = explode( '&', $tmpPar["lock_$i"] );
                                        $flag = TRUE;
                                        foreach( $locks as $lind => $linf ){
                                            if( $data[$info['dep']][$info['puesto']][$info['asesor']]['bono'][$tmpPar["par_$linf"]] == 0 ){
                                                $flag = FALSE;
                                            }
                                        }

                                        if( !$flag ){
                                            $data[$info['dep']][$info['puesto']][$info['asesor']]['bono'][$tmpPar["par_$i"]] = 0;
                                        }
                                    }
                                }
                            }

                            //Reductors
                            if( isset($tmpPar["red_par_$i"]) ){
                                if( $this->compare( $info[$tmpPar["red_par_$i"]], $tmpPar["red_comp_$i"], $tmpPar["red_op_$i"] ) ){

                                    if( $tmpPar["red_xInc_$i"] == 1 ){
                                        $data[$info['dep']][$info['puesto']][$info['asesor']]['reductores'][$tmpPar["red_par_$i"]] = $info[$tmpPar["red_par_$i"]] * $tmpPar["red_perc_$i"] * (-1);
                                    }else{
                                        $data[$info['dep']][$info['puesto']][$info['asesor']]['reductores'][$tmpPar["red_par_$i"]] = $tmpPar["red_perc_$i"] * (-1);
                                    }
                                }else{
                                    $data[$info['dep']][$info['puesto']][$info['asesor']]['reductores'][$tmpPar["red_par_$i"]] = 0;
                                }
                            }
                        }

                    }

                return array('data' => $data, 'deps' => $deps, 'params' => $p, 'detalle' => $res->result_array());
            }else{
                errResponse('Error al obtener data personal del asesor '.$asesor, REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error()); 
            }
    
    }
    
    public function prenomina_put(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){
            
            $params = $this->put();
            
            $nom = $this->db->query("SELECT IF(DAY(pago)>20,1,0) as bonoSearch, MONTH(ADDDATE(inicio,-30)) as mes, YEAR(ADDDATE(inicio,-30)) as anio FROM rrhh_calendarioNomina WHERE id=".$params['corte']);
            $nomRes = $nom->row_array();
            if( $nomRes['bonoSearch'] == 0 ){
                okResponse('Bonos Obtenidos', 'data', NULL, $this );
            }
            
            $month = $nomRes['mes'];
            $year = $nomRes['anio'];
            
            $data = $this->buildBonos( $month, $year );    
            
            $result = array();
            
            foreach( $data['data'] as $dep => $info ){
                foreach( $info as $puesto => $infoPuesto ){
                    if( isset( $data['params'][$dep][$puesto] ) ){
                        $bono = $infoPuesto['montoBono'];
                        foreach( $infoPuesto as $asesor => $infoAsesor ){
                            $result[$asesor] = 0;
                            if( is_array($infoAsesor) ){
                                if( intVal($infoAsesor['aplica']) == 1 &&  intVal($infoAsesor['aprobacion']['status']) == 1 ){
                                    foreach( $infoAsesor['bono'] as $par => $infoBono ){
                                        $result[$asesor] += $infoBono;
                                    }

                                    $tmpRed = 0;
                                    foreach( $infoAsesor['reductores'] as $red => $infoRed ){
                                        $tmpRed += $infoRed;
                                    }

                                    $tmpRed = $tmpRed < -1 ? 0 : 1+$tmpRed;

                                    $result[$asesor] = $result[$asesor] * $tmpRed;
                                }

                            }else{
                                $result[$asesor] = "NA";
                            }
                        }
                    }else{
                        continue;
                    }
                }
            }
                
                
            okResponse('Bonos Obtenidos', 'data', $result, $this, 'all', $data );

        }); 
    }
    
    public function chgStatus_put(){
        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            // $lpQ = $this->db->query("SELECT IF(allmighty=1,1,bonos_approve) as bonos_approve FROM userDB a LEFT JOIN profilesDB b ON a.profile=b.id WHERE asesor_id=".$_GET['usid']);
            $lpQ = $this->db->query("SELECT bonos_approve FROM userDB a LEFT JOIN profilesDB b ON a.profile=b.id WHERE asesor_id=".$_GET['usid']);
            $lpR = $lpQ->row_array();
            $lpLicense = $lpR['bonos_approve'];

            $data = $this->put();

            if( $lpLicense == 0 && $data['params']['review'] != 1 ){
                errResponse('No Cuentas con permisos para modificar este bono ', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', 'error'); 
            }
            
            
            $rev = $this->db->query("SELECT 
                    IF('".$data['lu']."' = Last_Update,
                        1,
                        IF('".$data['lu']."' BETWEEN ADDTIME(Last_Update, - '00:05:00') AND ADDTIME(Last_Update, '00:05:00')
                            AND approber = ".$_GET['usid'].",
                        1,
                        0)) AS luCheck, NOMBREASESOR(approber,1) as reviewer, review, review_notes as comments, a.status, Last_Update as lu
                FROM
                    asesores_bonos_aprobacion a
                WHERE
                    mes = ".$data['params']['mes']." AND anio = ".$data['params']['anio']." AND asesor = ".$data['params']['asesor']."");
            
            if( $rev->num_rows() > 0 ){
                $result = $rev->row_array();
                if( $result['luCheck'] != 1 ){
                    okResponse('Aprobacion Pendiente', 'data', array( 'status' => FALSE, 'msg' => "Este elemento fue modificado recientemente por ".$result['reviewer']), $this, 'meta', array('reviewer' => $result['reviewer'], 'review' => $result['review'], 'comments' => $result['comments'], 'status' => $result['status'], 'lu' => $result['lu']) );
                }
            }
            
            $data['params']['approber']=$_GET['usid'];
            
            $v = $this->db->query("SELECT IF(CURDATE()<=ADDDATE('".$data['params']['anio']."-".$data['params']['mes']."-19',30),1,0) as valid");
            $vRes = $v->row_array();
            
            if($vRes['valid'] != 1 && $_GET['usid'] != 170 ){
                errResponse('No es posible modificar el status después de la fecha de corte de nómina', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', false); 
            }
            
            if($this->db->set($data['params'])->insert('asesores_bonos_aprobacion')){
                okResponse('Aprobación Insertada', 'data', array('status' => true), $this );
            }else{
                if( $this->db->set($data['params'])
                    ->where(array('mes' => $data['params']['mes'], 'anio' => $data['params']['anio'], 'asesor' => $data['params']['asesor']))
                    ->update('asesores_bonos_aprobacion') ){
                    okResponse('Aprobación Actualizada', 'data', array('status' => true), $this );
                }else{
                    errResponse('Error al cambiar status del pago del bono', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error()); 
                }
            }

        }); 
    }


}
