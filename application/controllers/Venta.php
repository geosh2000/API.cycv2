<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
date_default_timezone_set('America/Mexico_city');
// use REST_Controller;


class Venta extends REST_Controller {

    public function __construct(){

        parent::__construct();
        $this->load->helper('json_utilities');
        $this->load->helper('validators');
        $this->load->helper('jwt');
        $this->load->database();
    }

     
    private function locs( $soloVenta, $td = FALSE ){
        
        $this->db->select("Fecha, Localizador, marca, pais, gpoCanalKpi")
                ->select("SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) as Total", FALSE)
                ->select("IF(SUM(VentaMXN)>0,Localizador,NULL) as NewLoc", FALSE)
                ->select("IF(SUM(VentaMXN)>=0 AND SUM(VentaMXN+OtrosIngresosMXN+EgresosMXN)>0,Localizador,NULL) as DaySum", FALSE)
                
                ->join("chanGroups b", "a.chanId = b.id", "left")
                ->where("marca", " @marca", FALSE)
                ->where("pais", " @pais", FALSE)
                ->where("gpoCanalKpi != ", "Agencias")
                ->group_by(array("Fecha", "Localizador"));
        
        if( $soloVenta == 1 ){
                $this->db->having("DaySum IS NOT ", "NULL", FALSE);
        }
        
            if( $td ){
            $this->db->from("d_Locs a")
                ->where("a.Fecha", " CURDATE()", FALSE);
        }else{
            $this->db->from("t_Locs a")
                ->where("(a.Fecha BETWEEN @inicio AND @fin OR
                        a.Fecha BETWEEN ADDDATE(@inicio,-364) AND ADDDATE(@fin, -364)) ", " ", FALSE);
            }
    }
    
    private function xHora( $td = FALSE ){
        
        $this->db->select("a.Fecha, tipoRsva, 
        CASE 
            WHEN gpoCanalKpi = 'PT.com' THEN IF(gpoTipoRsva = 'Presencial','In',gpoTipoRsva)
            WHEN gpoCanalKpi = 'Outlet' THEN 'Outlet'
            ELSE gpoTipoRsva
        END as gpoTipoRsvaOk")
              ->select("CAST(CONCAT(HOUR(a.Hora), 
                            CASE
                                WHEN MINUTE(a.Hora) >= 0  AND MINUTE(a.Hora)< 15 THEN ':00:00'
                                WHEN MINUTE(a.Hora) >= 15 AND MINUTE(a.Hora)< 30 THEN ':15:00'
                                WHEN MINUTE(a.Hora) >= 30 AND MINUTE(a.Hora)< 45 THEN ':30:00'
                                ELSE ':45:00'
                            END) as TIME) AS H", FALSE)
              ->select("Hora")
              ->select("SUM(VentaMXN+OtrosIngresosMXN+EgresosMXN) as Monto", FALSE)
              ->join("locs b", "a.Localizador = b.Localizador AND a.Fecha = b.Fecha", "right")
              ->join("dep_asesores c", "a.asesor = c.asesor AND a.Fecha = c.Fecha", "left")
              ->join("config_tipoRsva d", "IF(c.dep IS NULL, IF(a.asesor = - 1, - 1, 0), IF(c.dep NOT IN (0 , 3, 5, 29, 35, 50, 52), 0, c.dep)) = d.dep
                                            AND IF(a.tipo IS NULL OR a.tipo='',0, a.tipo) = d.tipo", "left", FALSE)
              ->group_by(array("a.Fecha", "H", "gpoTipoRsvaOk"))
              ->order_by("a.Fecha", "H");
        
         if( $td ){
            $this->db->from("d_Locs a")
                ->where("b.Fecha ", " CURDATE()", FALSE);
        }else{
            $this->db->from("t_Locs a")
                ->where("b.Fecha < ", " CURDATE()", FALSE);
         }
    }

    private function ventaMP($inicio, $fin, $type, $td=false, $mp = true){

        $this->db->query("DROP TEMPORARY TABLE IF EXISTS locs");

        if($type){
        $pdvType = "WHEN gpoCanalKpi = 'PDV' THEN 'PDV Presencial'";
        }else{
        $pdvType = "WHEN tipo = 1 THEN 'CC OUT'
                    WHEN tipo = 2 THEN 'PDV IN'
                    ELSE 'PDV Presencial'";
        }

        if($td){
        $table = "d_Locs";
        $fecha = "a.Fecha";
        $fechaVar = "CURDATE()";
        }else{
        $table = "t_Locs";
        $fecha = "a.Fecha BETWEEN";
        $fechaVar = "'$inicio' AND '$fin'";
        }

        $this->db->select("a.*, canal, gpoCanal, gpoCanalKpi, marca, pais, tipoCanal, dep, vacante, puesto")
                ->select("case
                                WHEN gpoCanalKpi = 'PDV' THEN
                            CASE
                                $pdvType
                            END
                                WHEN a.asesor>=0 THEN
                                    CASE
                                        WHEN dep IN (5,52) THEN
                                            CASE
                                                WHEN tipo = 2 THEN 'CC IN'
                                                ELSE 'CC OUT'
                                            end
                                        WHEN dep = 29 THEN 
                                            CASE 
                                                WHEN d.cc LIKE '%mixcoac%' THEN 'Mixcoac'
                                                ELSE 'PDV IN'
                                            END
                                        ELSE 'CC IN'
                                    end
                                ELSE 'Online'
                            end gpoInterno", FALSE)
                ->select("IF(VentaMXN>0, Localizador, NULL) as NewLoc", FALSE)
                ->from("$table a")
                ->join("chanGroups b", "a.chanId = b.id", "left")
                ->join("dep_asesores c", "a.asesor = c.asesor AND a.Fecha = c.Fecha", "left")
                ->join("cc_apoyo d", "a.asesor = d.asesor AND a.Fecha BETWEEN d.inicio AND d.fin", "left")
                ->where($fecha, $fechaVar, FALSE);
        
        if( $mp ){
            $this->db->where( array( 'marca' => 'Marcas Propias', 'pais' => 'MX', 'gpoCanalKPI !=' => 'Outlet' ) );
        }else{
            $this->db->where( array( 'marca' => 'Marcas Terceros', 'gpoCanalKpi !=' => 'Agencias', 'gpoCanalKpi !=' => 'Avianca') );
        }
                

        $tableLocs = $this->db->get_compiled_select();

        IF($this->db->query("CREATE TEMPORARY TABLE locs $tableLocs")){

        return true;
        }else{
        errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }


    }

    private function ventaProducto($inicio, $fin, $type, $td=false, $mp = true){

        $this->db->query("DROP TEMPORARY TABLE IF EXISTS locsProd");

        if($td){
        $table = "t_hoteles_test";
        $fecha = "a.Fecha";
        $fechaVar = "CURDATE()";
        }else{
        $table = "t_hoteles_test";
        $fecha = "a.Fecha BETWEEN";
        $fechaVar = "'$inicio' AND '$fin'";
        }

        $this->db->select("a.*, canal, gpoCanal, gpoCanalKpi, marca, pais, tipoCanal")
                ->select("IF(VentaMXN>0, Localizador, NULL) as NewLoc", FALSE)
                ->from("$table a")
                ->join("chanGroups b", "a.chanId = b.id", "left")
                ->where($fecha, $fechaVar, FALSE);

        if( $mp ){
            $this->db->where( array( 'marca' => 'Marcas Propias', 'pais' => 'MX' ) );
        }else{
            $this->db->where( array( 'marca' => 'Marcas Terceros', 'gpoCanalKpi !=' => 'Agencias', 'gpoCanalKpi !=' => 'Avianca') );
        }

        $tableLocs = $this->db->get_compiled_select();

        IF($this->db->query("CREATE TEMPORARY TABLE locsProd $tableLocs")){    
       
        return true;
        }else{
        errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }


    }

    public function getVentaPorCanalSV_get(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            // ======================================================================
            // START Get Inputs
            // ======================================================================
                $start = $this->uri->segment(3);
                $end = $this->uri->segment(4);
                $sv = $this->uri->segment(5);
                $type = $this->uri->segment(6);
                $td = $this->uri->segment(7);
                $prodIn = $this->uri->segment(8);
                $pq = $this->uri->segment(9);
                $ml = $this->uri->segment(10);
                $h = $this->uri->segment(11);
                $mt = $this->uri->segment(12);
            // ======================================================================
            // END Get Inputs
            // ======================================================================

            // ======================================================================
            // START Validación de Inputs
            // ======================================================================
                segmentSet( 3, 'Debes ingresar una fecha de inicio', $this );
                segmentSet( 4, 'Debes ingresar una fecha de fin', $this );
                segmentType( 3, "El input debe ser de tipo 'Fecha' en formato YYYY-MM-DD", $this, 'date' );
                segmentType( 4, "El input debe ser de tipo 'Fecha' en formato YYYY-MM-DD", $this, 'date' );
            // ======================================================================
            // END Validación de Inputs
            // ======================================================================
            
            // ======================================================================
            // START Parameters
            // ======================================================================
                $t = $type == 1 ? true : false;
                $td = $td == 1 ? true : false;
                $prod = $prodIn == 1 ? true : false;
                $isPaq = $pq == 'true' ? "WHEN isPaq != 0 THEN 'Paquete'" : "";
                $mp = isset($mt) && $mt==1 ? false : true;

                if( $h == 1 ){
                    $end = $start;
                }
                $isHour = $h == 1 ? true : false;

            // ======================================================================
            // END Parameters
            // ======================================================================

            // ======================================================================
            // START Query for HOURLY
            // ======================================================================
                if( $h == 1 ){
                    $this->db->query("DROP TEMPORARY TABLE IF EXISTS porHora");
                    $this->db->query("CREATE TEMPORARY TABLE porHora 
                                            SELECT 
                                                Localizador, HOUR(Hora) + IF(MINUTE(Hora) >= 30, .5, 0) AS H
                                            FROM
                                                t_Locs
                                            WHERE
                                                Fecha = '$start' AND VentaMXN > 0 GROUP BY Localizador");
                    $this->db->query("ALTER TABLE porHora ADD PRIMARY KEY (Localizador)");
                }
            // ======================================================================
            // END Query for HOURLY
            // ======================================================================
                
            // ======================================================================
            // START Venta Query
            // ======================================================================
                if($this->ventaMP($start, $end, $t, $td, $mp)){

                    $this->db->query("DROP TEMPORARY TABLE IF EXISTS soloVenta");

                    if($sv == 1){
                        $qSV = "IF((SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) < 0
                                        AND NewLoc IS NOT NULL)
                                        OR SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) >= 0,
                                    SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN),
                                    0) as Monto,
                                IF((SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) < 0
                                        AND NewLoc IS NOT NULL)
                                        OR SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) >= 0,
                                    NewLoc,NULL) as LocCount,";
                    }else{
                        $qSV = "SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) as Monto, Localizador as LocCount,";
                    }

                    $this->db->select("Fecha, Localizador, gpoInterno")
                            ->select($qSV, FALSE)
                            ->from("locs")
                            ->group_by('Fecha, Localizador');

                    $soloVenta = $this->db->get_compiled_select();
                    $this->db->query("CREATE TEMPORARY TABLE soloVenta $soloVenta");
                    $this->db->query("ALTER TABLE soloVenta
                                    ADD PRIMARY KEY (Fecha, Localizador)");

                    if($prod){

                        if($this->ventaProducto($start, $end, $t, $td, $mp)){
                            $this->db->query("DROP TEMPORARY TABLE IF EXISTS soloVentaProd");

                            $this->db->select("Fecha, Localizador, item, itemType, categoryId, isPaq")
                                    ->select($qSV, FALSE)
                                    ->from("locsProd")
                                    ->group_by('Fecha, Localizador, item');

                            $soloVentaProd = $this->db->get_compiled_select();
                            $this->db->query("CREATE TEMPORARY TABLE soloVentaProd $soloVentaProd");
                            $this->db->query("ALTER TABLE soloVentaProd
                                            ADD PRIMARY KEY (Fecha, Localizador, item)");

                            $this->db->query("DROP TEMPORARY TABLE IF EXISTS prod");

                            $this->db->select("a.*, gpoInterno")
                                    ->select("CASE
                                                $isPaq
                                                WHEN itemType = 0 THEN 'None'
                                                WHEN itemType = 1 THEN 'Hotel'
                                                WHEN itemType = 2 THEN 'Transfer'
                                                WHEN itemType = 3 THEN 'Vuelo'
                                                WHEN itemType = 4 THEN 'Tour'
                                                WHEN itemType = 5 THEN 'Auto'
                                                WHEN itemType = 6 THEN 'Paquete'
                                                WHEN itemType = 7 THEN 'ServiceCharge'
                                                WHEN itemType = 8 THEN 'Bus'
                                                WHEN itemType = 11 THEN 'Crucero'
                                                WHEN itemType = 12 THEN 'Seguro'
                                                WHEN itemType = 13 THEN 'Circuito'
                                                WHEN itemType = 14 THEN
                                                CASE
                                                    WHEN categoryId = 0 THEN 'None'
                                                    WHEN categoryId = 1 THEN 'Hotel'
                                                    WHEN categoryId = 6 THEN 'Transfer'
                                                    WHEN categoryId = 3 THEN 'Vuelo'
                                                    WHEN categoryId = 7 THEN 'Tour'
                                                    WHEN categoryId = 8 THEN 'Auto'
                                                    WHEN categoryId = 0 THEN 'Paquete'
                                                    WHEN categoryId = 14 THEN 'ServiceCharge'
                                                    WHEN categoryId = 9 THEN 'Bus'
                                                    WHEN categoryId = 2 THEN 'Crucero'
                                                    WHEN categoryId = 4 THEN 'Seguro'
                                                    WHEN categoryId = 10 THEN 'Circuito'
                                                    WHEN categoryId = 5 THEN 'Generico'
                                                    ELSE 'Otro'
                                                END
                                                ELSE 'Otro'
                                            END as iType", FALSE)
                                    ->from("soloVentaProd a")
                                    ->join("soloVenta b", "a.Fecha = b.Fecha AND a.Localizador = b.Localizador", "left")
                                    ->where("b.Localizador IS NOT ", "NULL", FALSE);

                                    $prodQ = $this->db->get_compiled_select();
                            
                            
                                    if($this->db->query("CREATE TEMPORARY TABLE prod $prodQ")){
                                    $this->db->select('Fecha, gpoInterno, iType as producto, SUM(Monto) as Monto, COUNT(DISTINCT LocCount) as Locs', FALSE)
                                            ->from('prod a')
                                            ->group_by('Fecha, gpoInterno, producto');
                                    }else{
                                    errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', array($this->db->error(),$prodQ));
                                    }

                        }

                    }else{
                        $this->db->select('Fecha, gpoInterno, SUM(Monto) as Monto, COUNT(DISTINCT LocCount) as Locs', FALSE)
                            ->from('soloVenta a')
                            ->group_by('Fecha, gpoInterno');
                    }

                    // ======================================================================
                    // START Query for HOURLY
                    // ======================================================================
                        if( $h == 1 ){
                            $this->db->select('b.h')
                                    ->join('porHora b', 'a.LocCount = b.Localizador', 'left')
                                    ->group_by('b.h')
                                    ->order_by('h');
                        }
                    // ======================================================================
                    // END Query for HOURLY
                    // ======================================================================

                    if($q = $this->db->get()){
                        $result = $q->result_array();
                        
                        foreach($result as $index => $info){
                            if($info['Monto'] == NULL){
                                $monto = 0;
                            }else{
                                $monto = $info['Monto'];
                            }

                            if( $prod ){
                                $type = $h ? 'ph' : 'pd';
                            }else{
                                $type = $h ? 'h' : 'd';
                            }

                            switch($type){
                                case 'h':
                                    $dataRes[$info['Fecha']][$info['h']][$info['gpoInterno']]=floatVal($monto);
                                    $dataLocs[$info['Fecha']][$info['h']][$info['gpoInterno']]=intVal($info['Locs']);
                                    break;
                                case 'd':
                                    $dataRes[$info['Fecha']][$info['gpoInterno']]=floatVal($monto);
                                    $dataLocs[$info['Fecha']][$info['gpoInterno']]=intVal($info['Locs']);
                                    break;
                                case 'ph':
                                    $dataRes[$info['Fecha']][$info['h']][$info['producto']][$info['gpoInterno']]=floatVal($monto);
                                    $dataLocs[$info['Fecha']][$info['h']][$info['producto']][$info['gpoInterno']]=intVal($info['Locs']);
                                    break;
                                case 'pd':
                                    $dataRes[$info['Fecha']][$info['producto']][$info['gpoInterno']]=floatVal($monto);
                                    $dataLocs[$info['Fecha']][$info['producto']][$info['gpoInterno']]=intVal($info['Locs']);
                                    break;
                            }

                        }

                        $luQ = $this->db->query("SELECT MAX(Last_Update) as lu FROM d_Locs WHERE Fecha=CURDATE()");
                        $luR = $luQ->row_array();
                        $lu = $luR['lu'];

                        okResponse( 'Data obtenida', 'data', array('venta' => $dataRes, 'locs' => $dataLocs), $this, 'lu', $lu );
                    }else{
                        errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
                    }

                    return true;

                }else{
                    errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', 'Error al generar base de Ventas');
                }
            // ======================================================================
            // END Venta Query
            // ======================================================================

        });

        jsonPrint( $result );

    }

    public function getRN_post(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $data = $this->post();

        $this->db->query("SET @inicio = CAST('".$data['start']."' as DATE)");
        $this->db->query("SET @fin    = CAST('".$data['end']."' as DATE)");
        $this->db->query("SET @pais   = '".$data['pais']."'");
        $this->db->query("SET @marca  = '".$data['marca']."'");

        $this->db->query("DROP TEMPORARY TABLE IF EXISTS hotelesRAW");

        $this->db->select("a.*", FALSE)
                ->select("if(VentaMXN>0,CONCAT(Localizador,"-",item),null) as NewLoc", FALSE)
                ->select("IF(tipoCanal = 'Movil', 'Online', tipoCanal) as tipoCanal", FALSE)
                ->select("gpoCanalKpi")
                ->from("t_hoteles_test a")
                ->join("chanGroups b", "a.chanId = b.id", "left")
                ->where("Fecha BETWEEN ", "@inicio AND IF(@fin>CURDATE(),CURDATE(),@fin)", FALSE)
                ->where(array( 'categoryId' => 1, 'pais' => $data['pais'], 'marca' => $data['marca'] ));

        $hotelesRAW = $this->db->get_compiled_select();

        if( $this->db->query("CREATE TEMPORARY TABLE hotelesRAW $hotelesRAW") ){

            $this->db->query("ALTER TABLE hotelesRAW ADD PRIMARY KEY (`Localizador`, `Fecha`, `Hora`, `item`)");
            $this->db->query("SELECT @maxDate := MAX(Fecha) FROM hotelesRAW");

            $this->db->select("a.*", FALSE)
                    ->select("if(VentaMXN>0,CONCAT(Localizador,"-",item),null) as NewLoc", FALSE)
                    ->select("IF(tipoCanal = 'Movil', 'Online', tipoCanal) as tipoCanal", FALSE)
                    ->select("gpoCanalKpi")
                    ->from("t_hoteles_test a")
                    ->join("chanGroups b", "a.chanId = b.id", "left")
                    ->where("Fecha BETWEEN ", "IF(@maxDate IS NULL, @inicio, @maxDate) AND IF(@fin>CURDATE(),CURDATE(),@fin)", FALSE)
                    ->where(array( 'categoryId' => 1, 'pais' => $data['pais'], 'marca' => $data['marca'] ));

            $hotelesRAW = $this->db->get_compiled_select();

            if( $this->db->query("INSERT INTO hotelesRAW (SELECT * FROM ($hotelesRAW) a) ON DUPLICATE KEY UPDATE VentaMXN = a.VentaMXN") ){

            $this->db->select("Fecha, gpoCanalKpi, tipoCanal, SUM(clientNights) as RN_w_xld, SUM(IF(clientNights>0,clientNights,0)) as RN", FALSE)
                    ->from('hotelesRAW')
                    ->group_by("Fecha, gpoCanalKpi, tipoCanal");

            if( $dates = $this->db->get() ){

                $this->db->select("gpoCanalKpi, tipoCanal, SUM(clientNights) as RN_w_xld, SUM(IF(clientNights>0,clientNights,0)) as RN", FALSE)
                        ->from('hotelesRAW')
                        ->group_by("gpoCanalKpi, tipoCanal");

                if( $all = $this->db->get() ){

                $luq = $this->db->query("SELECT MAX(Last_Update) as LU FROM t_hoteles_test WHERE Fecha = CURDATE()");

                $data = array( 'dates' => $dates->result_array(), 'all' => $all->result_array(), 'lu' => $luq->row_array());

                okResponse( 'Data obtenida', 'data', $data, $this, 'lu', $lu );

                }else{
                errResponse('Error al compilar data por Rango', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
                }

            }else{
                errResponse('Error al compilar data por Fecha', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }


            }else{
            errResponse('Error al insertar data actual a hotelesRAW', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

        }else{
            errResponse('Error al compilar información hotelesRAW', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }



            return true;

        });

        jsonPrint( $result );

    }
     
    public function fc_put(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $data = $this->put();
            
            $tdQ = $this->db->select("CURDATE() as fecha")->get();
            $td = $tdQ->row_array();
            
            if( $td['fecha'] == $data['Fecha'] ){
                $this->db->from("d_Locs a");
            }else{
                $this->db->from("t_Locs a");
            }
            
            switch($data['skill']){
                case 35:
                    $this->db->where('marca', 'Marcas Propias');
                    break;
                case 3:
                    $this->db->where('marca', 'Marcas Terceros');
                    break;
                default:
                    errResponse('No es posible obtener información del skill '.$data['skill'], REST_Controller::HTTP_BAD_REQUEST, $this );
                    break;
            }
            
            $this->db->select("Localizador, tipoRsva")
                ->select("SUM(VentaMXN+OtrosIngresosMXN+EgresosMXN) as Monto, IF(SUM(VentaMXN)>0 OR SUM(VentaMXN+OtrosIngresosMXN+EgresosMXN)>0, Localizador, NULL) as NewLoc", FALSE)
                ->join("chanGroups b", "a.chanId = b.id", "left")
                ->join("dep_asesores c", "a.asesor = c.asesor AND a.Fecha = c.Fecha", "left")
                ->join("config_tipoRsva d", "IF(c.dep IS NULL, IF(a.asesor = - 1, - 1, 0), IF(c.dep NOT IN (0 , 3, 5, 29, 35, 50, 52), 0, c.dep)) = d.dep
                                                AND IF(a.tipo IS NULL OR a.tipo='',0, a.tipo) = d.tipo", "left", FALSE)
                ->where("a.Fecha", $data['Fecha'])
                ->group_by('Localizador')
                ->having('tipoRsva IS NOT ', 'NULL', FALSE);
            
            $locs = $this->db->get_compiled_select();
            
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS locs");
            $this->db->query("CREATE TEMPORARY TABLE locs $locs");
            
            if( $q = $this->db->query("SELECT tipoRsva, COUNT(DISTINCT NewLoc) as locs FROM locs WHERE tipoRsva LIKE '%In%' GROUP BY tipoRsva") ){
                okResponse( 'Data obtenida', 'data', $q->result_array(), $this );
            }else{
                errResponse('Error al compilar información locs', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }
            


            return true;

        });

        jsonPrint( $result );

    }  
    
    public function dashPorHora_put(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

        $data = $this->put();

        $this->db->query("SET @inicio = CAST('".$data['start']."' as DATE)");
        $this->db->query("SET @fin    = CAST('".$data['end']."' as DATE)");
        $this->db->query("SET @pais   = '".$data['pais']."'");
        $this->db->query("SET @marca  = '".$data['marca']."'");

        $this->db->query("DROP TEMPORARY TABLE IF EXISTS locs");

        $this->locs( intVal($data['soloVenta']) );

        $locs = $this->db->get_compiled_select();

        if( $this->db->query("CREATE TEMPORARY TABLE locs $locs") ){
            
            $this->db->query("ALTER TABLE locs ADD PRIMARY KEY(Fecha,Localizador)");
            
            if( date('Y-m-d') <= date('Y-m-d', strtotime($data['start'])) && date('Y-m-d') >= date('Y-m-d', strtotime($data['start'])) ){
                $this->locs( intVal($data['soloVenta']), TRUE );
                $tdLocs = $this->db->get_compiled_select();

                $this->db->query("INSERT INTO locs (SELECT * FROM ( $tdLocs ) a ) ON DUPLICATE KEY UPDATE marca = a.marca");
            }
            
            
            $this->xHora();
            $xHora = $this->db->get_compiled_select();
            
            if( $this->db->query("CREATE TEMPORARY TABLE xHora $xHora") ){
                
                $this->db->query("ALTER TABLE xHora ADD PRIMARY KEY(Fecha, H, gpoTipoRsvaOk)");
                
                if( date('Y-m-d') <= date('Y-m-d', strtotime($data['start'])) && date('Y-m-d') >= date('Y-m-d', strtotime($data['start'])) ){
                    $this->xHora( TRUE );
                    $xHoraTd = $this->db->get_compiled_select();

                    $this->db->query("INSERT INTO xHora (SELECT * FROM ( $xHoraTd ) a ) ON DUPLICATE KEY UPDATE Monto = a.Monto");
                }
                
                
                $q = $this->db->get('xHora');
                $l = $this->db->query("SELECT MAX(Last_Update) as lu FROM d_Locs WHERE Fecha>=ADDDATE(CURDATE(),-1)");
                
                
                okResponse( 'Data obtenida', 'data', $q->result_array(), $this, 'lu', $l->row_array() );
            }else{
                errResponse('Error al compilar información final', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

        }else{
            errResponse('Error al compilar información locs', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
        }



            return true;

        });

        jsonPrint( $result );

    }
       
    public function kpis_put(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            // =================================================
            // START GET PARAMS
            // =================================================
                $data = $this->put();
            // =================================================
            // END GET PARAMS
            // =================================================

            // =================================================
            // START SET PARAMS FOR QUERY
            // =================================================
                $this->db->query("SET @fecha = CAST('".$data['Fecha']."' as DATE)");
                $this->db->query("SET @pais   = '".$data['pais']."'");
                $this->db->query("SET @marca  = '".$data['marca']."'");

                // Define hour for historic views
                if( $data['h'] == 1 ){
                    $this->db->query("SET @hora  = '23:59:59'");  
                }else{
                    $this->db->query("SET @hora  = CAST('".$data['Hora']."' as TIME)");
                }
            // =================================================
            // END SET PARAMS FOR QUERY
            // =================================================
                
            // =================================================
            // START LOCS QUERY
            // =================================================
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS locs");
                $this->db->select("a.*, gpoCanalKpi")
                    ->from("t_Locs a")
                    ->join("chanGroups b", "a.chanId = b.id", "left")
                    ->where('gpoCanalKpi !=', 'Agencias')
                    ->where("marca = @marca
                                AND (Fecha = @fecha
                                OR Fecha = ADDDATE(@fecha, - 364)
                                OR Fecha = ADDDATE(@fecha, - 7)
                                OR Fecha = ADDDATE(@fecha, - 1)) AND Hora <= @hora ", "", FALSE);
                
                if( $data['marca'] == 'Marcas Propias' ){
                    $this->db->where("pais", "@pais", FALSE);
                }
                
                $locs = $this->db->get_compiled_select();
                $this->db->query("CREATE TEMPORARY TABLE locs $locs");
                $this->db->query("ALTER TABLE locs ADD PRIMARY KEY (`Localizador`, `Venta`, `Fecha`, `Hora`)");
                    
                // INSERT TODADY RESULTS
                if( date('Y-m-d') == $data['Fecha'] ){
                    $this->db->select("a.*, gpoCanalKpi")
                    ->from("d_Locs a")
                    ->join("chanGroups b", "a.chanId = b.id", "left")
                    ->where('gpoCanalKpi !=', 'Agencias')
                    ->where("marca = @marca
                                AND Fecha = @fecha AND Hora <= @hora ", "", FALSE);
                    
                    if( $data['marca'] == 'Marcas Propias' ){
                        $this->db->where("pais", "@pais", FALSE);
                    }
                    
                    $locs = $this->db->get_compiled_select();
                    $this->db->query("INSERT INTO locs SELECT * FROM ($locs) a ON DUPLICATE KEY UPDATE asesor=a.asesor, tipo=a.tipo");
                }
            // =================================================
            // END LOCS QUERY
            // =================================================
                
            // =================================================
            // START Define New Locs, Count Locs and Modified Locs
            // =================================================
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS locsCount");
                
                $this->db->select("Fecha, Localizador, asesor, tipo")
                    ->select("SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS Monto,
                                    IF(SUM(VentaMXN) > 0, Localizador, NULL) AS NewLoc,
                                    IF(SUM(VentaMXN) > 0
                                            AND SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) > 0,
                                        Localizador,
                                        NULL) AS CountLoc,
                                    IF(SUM(VentaMXN) >= 0
                                            OR SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) > 0,
                                        Localizador,
                                        NULL) AS ModifLoc", FALSE)
                    ->from('locs')
                    ->group_by(array("Fecha", "Localizador"));
                $locsCount = $this->db->get_compiled_select();
                $this->db->query("CREATE TEMPORARY TABLE locsCount $locsCount");
                $this->db->query("ALTER TABLE locsCount ADD PRIMARY KEY (`Localizador`, `Fecha`)");
            // =================================================
            // END Define New Locs, Count Locs and Modified Locs
            // =================================================
                
            // =================================================
            // START Locs Summary
            // =================================================
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS locsOK");
                
                $this->db->select("a.*, NewLoc, CountLoc, ModifLoc, tipoRsva, d.Monto, gpoTipoRsva", FALSE)
                    ->from("locs a")
                    ->join("dep_asesores b", "a.asesor = b.asesor AND a.Fecha = b.Fecha", "left", FALSE)
                    ->join("config_tipoRsva c", "IF(a.tipo IS NULL OR a.tipo='',0, a.tipo) = c.tipo
                                                AND IF(b.dep IS NULL,
                                                IF(a.asesor = - 1, - 1, 0),
                                                IF(b.dep NOT IN (0 , 3, 5, 29, 35, 50, 52),
                                                    0,
                                                    b.dep)) = c.dep", "left", FALSE)
                    ->join("locsCount d", "a.Localizador = d.Localizador AND a.Fecha=d.Fecha", "left", FALSE);
                $locsOK = $this->db->get_compiled_select();
                $this->db->query("CREATE TEMPORARY TABLE locsOK $locsOK");
            // =================================================
            // END Locs Summary
            // =================================================
                
            // =================================================
            // START QUERY T_HOT
            // =================================================
                $this->db->select('a.*, gpoCanalKpi')
                ->from("t_hoteles_test a")
                ->join("chanGroups b", "a.chanId = b.id", "left")
                ->where('gpoCanalKpi !=', 'Agencias')
                ->where("marca = @marca
                AND (Fecha = @fecha
                OR Fecha = ADDDATE(@fecha, - 364)
                OR Fecha = ADDDATE(@fecha, - 7)
                OR Fecha = ADDDATE(@fecha, - 1)) AND Hora <= @hora ", "", FALSE);
                
                if( $data['marca'] == 'Marcas Propias' ){
                    $this->db->where("pais", "@pais", FALSE);
                }
                
                $tHot = $this->db->get_compiled_select();
                
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS tHot");
                $this->db->query("CREATE TEMPORARY TABLE tHot $tHot");
                $this->db->query("ALTER TABLE tHot ADD PRIMARY KEY (`Localizador`, `Fecha`, `item`, `Hora`)");
            // =================================================
            // END QUERY T_HOT
            // =================================================
            
            // =================================================
            // START SERVICIOS SUMMARY
            // =================================================
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS servicios");
                if( $data['paq'] == 1 ){
                    $this->db->select("i.servicio");
                }else{
                    $this->db->select("IF(isPaq = 0, i.servicio, 'Paquete') as servicio", FALSE);
                }
                
                $this->db->select("h.*, NewLoc, CountLoc, ModifLoc, tipoRsva, d.Monto, gpoTipoRsva")
                    ->from("tHot h")
                    ->join("locsCount d", "h.Localizador = d.Localizador AND h.Fecha=d.Fecha", "left", FALSE)
                    ->join("dep_asesores b", "d.asesor = b.asesor AND h.Fecha = b.Fecha", "left", FALSE)
                    ->join("config_tipoRsva c", "IF(d.tipo IS NULL OR d.tipo='',0, d.tipo) = c.tipo
                                                AND IF(b.dep IS NULL,
                                                IF(d.asesor = - 1, - 1, 0),
                                                IF(b.dep NOT IN (0 , 3, 5, 29, 35, 50, 52),
                                                    0,
                                                    b.dep)) = c.dep", "left", FALSE)
                    ->join("itemTypes i", "h.itemType = i.type AND h.categoryId = i.category", "left", FALSE)
                    ->having("Localizador IS NOT ", "NULL", FALSE);

                $servicios = $this->db->get_compiled_select();
                $this->db->query("CREATE TEMPORARY TABLE servicios $servicios");
            // =================================================
            // END SERVICIOS SUMMARY
            // =================================================
                
                
                if( $data['marca'] == 'Marcas Propias' ){
                $this->db->select("Fecha, gpoCanalKpi as gpoCanalKpiOK, IF(gpoCanalKpi = 'PT.com' AND gpoTipoRsva = 'Presencial','In',gpoTipoRsva) as gpoTipoRsvaOk", FALSE);
                }else{
                $this->db->select("Fecha, IF(gpoCanalKpi = 'Afiliados', gpoCanalKpi, 'Afiliados') as gpoCanalKpiOK, IF(gpoCanalKpi = 'PT.com' AND gpoTipoRsva = 'Presencial','In',gpoTipoRsva) as gpoTipoRsvaOk", FALSE);
                }
                
                
                $this->db->select("COUNT(DISTINCT CountLoc) AS Locs,
                                SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) MontoAll,
                                SUM(IF(CountLoc IS NOT NULL,
                                    VentaMXN + OtrosIngresosMXN + EgresosMXN,
                                    0)) AS MontoSV,
                                SUM(IF(NewLoc IS NULL AND Monto < 0,
                                    VentaMXN + OtrosIngresosMXN + EgresosMXN,
                                    0)) XldAll", FALSE)
                    ->from("locsOK")
                    ->group_by(array("Fecha", "gpoCanalKpiOK", "gpoTipoRsvaOk"))
                    ->order_by("gpoCanalKpiOk DESC, Fecha", FALSE);
                    
                
                if( $l = $this->db->get() ){
                    
                    if( $data['marca'] == 'Marcas Propias' ){
                    $this->db->select("Fecha, gpoCanalKpi as gpoCanalKpiOK, IF(gpoCanalKpi = 'PT.com' AND gpoTipoRsva = 'Presencial','In',gpoTipoRsva) as gpoTipoRsvaOk, servicio", FALSE);
                    }else{
                    $this->db->select("Fecha, IF(gpoCanalKpi = 'Afiliados', gpoCanalKpi, 'Afiliados') as gpoCanalKpiOK, IF(gpoCanalKpi = 'PT.com' AND gpoTipoRsva = 'Presencial','IN',gpoTipoRsva) as gpoTipoRsvaOk, servicio", FALSE);
                    }
                    
                    $this->db->select("COUNT(DISTINCT CountLoc) AS Locs,
                                SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) MontoAll,
                                SUM(IF(CountLoc IS NOT NULL, VentaMXN + OtrosIngresosMXN + EgresosMXN,
                                    0)) AS MontoSV,
                                SUM(IF(NewLoc IS NULL AND Monto < 0,
                                    VentaMXN + OtrosIngresosMXN + EgresosMXN,
                                    0)) XldAll,
                                SUM(clientNights) as newRN,
                                SUM(clientNights) as allRN", FALSE)
                    ->from("servicios")
                    ->group_by(array("Fecha", "gpoCanalKpiOK", "gpoTipoRsvaOk", "servicio"))
                        ->order_by("servicio, gpoCanalKpiOk DESC, Fecha", FALSE);
                    
                    
                    if( $s = $this->db->get() ){
                        $lu = $this->db->query("SELECT MAX(Last_Update) as lu FROM d_Locs WHERE Fecha=CURDATE()");
                        $LU = $lu->row_array();
                        okResponse( 'Data obtenida', 'data', array( 'locs' => $l->result_array(), 'servicios' => $s->result_array()), $this, 'lu', $LU['lu'] );
                    }else{
                        errResponse('Error al compilar información servicios', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
                    }
                }else{
                    errResponse('Error al compilar información locs', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
                }

        });

        jsonPrint( $result );

    } 
    
    public function kpisPdv_put(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $data = $this->put();

            // =================================================
            // START PARAMETROS
            // =================================================
                $this->db->query("SET @fecha = CAST('".$data['Fecha']."' as DATE)");
                $this->db->query("SET @pais   = '".$data['pais']."'");
                $this->db->query("SET @marca  = '".$data['marca']."'");

                if( $data['h'] == 1 ){
                    // Para producción
                    $this->db->query("SET @hora  = '23:59:59'");  
                    
                    // Prueba para horario específico
                    // $this->db->query("SET @hora  = CAST('20:00:00' as TIME)");
                }else{
                    // Para producción
                    $this->db->query("SET @hora  = CAST('".$data['Hora']."' as TIME)");

                    // Prueba para horario específico
                    // $this->db->query("SET @hora  = CAST('19:00:00' as TIME)");
                }
            // =================================================
            // END PARAMETROS
            // =================================================
            
            // =================================================
            // START LOCS QUERY
            // =================================================
            
                // =================================================
                // START HISTORIC LOCS
                // =================================================
                    $this->db->query("DROP TEMPORARY TABLE IF EXISTS locs");
                    $this->db->select("a.*, gpoCanalKpi, c.displayNameShort as branch, cityForListing as city, FindSuperDayPDV(@fecha, p.id, 2) as super")
                        ->from("t_Locs a")
                        ->join("chanGroups b", "a.chanId = b.id", "left")
                        ->join("cat_branch c", "a.branchid = c.branchId", "left")
                        ->join("PDVs p", "a.branchid = p.branchId", "left")
                        // Definir el canal
                        // ->where('gpoCanalKpi', 'PDV')
                        ->where("marca = @marca
                                    AND (Fecha = @fecha
                                    OR Fecha = ADDDATE(@fecha, - 364)
                                    OR Fecha = ADDDATE(@fecha, - 7)
                                    OR Fecha = ADDDATE(@fecha, - 1)) AND Hora <= @hora ", "", FALSE);

                    if( $data['marca'] == 'Marcas Propias' ){
                        $this->db->where("pais", "@pais", FALSE);
                    }

                    $locs = $this->db->get_compiled_select();
                    $this->db->query("CREATE TEMPORARY TABLE locs $locs");
                    $this->db->query("ALTER TABLE locs ADD PRIMARY KEY (`Localizador`, `Venta`, `Fecha`, `Hora`)");
                // =================================================
                // END HISTORIC LOCS
                // =================================================

                // =================================================
                // START TODAY LOCS
                // =================================================
                    if( date('Y-m-d') == $data['Fecha'] ){
                        $this->db->select("a.*, gpoCanalKpi, c.displayNameShort as branch, cityForListing as city, FindSuperDayPDV(@fecha, p.id, 2) as super")
                        ->from("d_Locs a")
                        ->join("chanGroups b", "a.chanId = b.id", "left")
                        ->join("cat_branch c", "a.branchid = c.branchId", "left")
                            ->join("PDVs p", "a.branchid = p.branchId", "left")
                        // Definir el canal
                        // ->where('gpoCanalKpi', 'PDV')
                        ->where("marca = @marca
                                    AND Fecha = @fecha AND Hora <= @hora ", "", FALSE);

                        if( $data['marca'] == 'Marcas Propias' ){
                            $this->db->where("pais", "@pais", FALSE);
                        }

                        $locs = $this->db->get_compiled_select();
                        $this->db->query("INSERT INTO locs SELECT * FROM ($locs) a ON DUPLICATE KEY UPDATE asesor=a.asesor, tipo=a.tipo");
                    }
                // =================================================
                // END TODAY LOCS
                // =================================================
            // =================================================
            // END LOCS QUERY
            // =================================================

            // =================================================
            // START Define New Locs, Count Locs and Modified Locs
            // =================================================
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS locsCount");

                $this->db->select("Fecha, Localizador, asesor, tipo")
                    ->select("SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS Monto,
                                    IF(SUM(VentaMXN) > 0, Localizador, NULL) AS NewLoc,
                                    IF(SUM(VentaMXN) >= 0
                                            AND SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) > 0,
                                        Localizador,
                                        NULL) AS CountLoc,
                                    IF(SUM(VentaMXN) > 0
                                            OR SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) > 0,
                                        Localizador,
                                        NULL) AS ModifLoc", FALSE)
                    ->from('locs a')
                    ->group_by(array("Fecha", "Localizador"));
                $locsCount = $this->db->get_compiled_select();
                $this->db->query("CREATE TEMPORARY TABLE locsCount $locsCount");
                $this->db->query("ALTER TABLE locsCount ADD PRIMARY KEY (`Localizador`, `Fecha`)");
            // =================================================
            // END Define New Locs, Count Locs and Modified Locs
            // =================================================

            // =================================================
            // START LOCS SUMMARY
            // =================================================
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS locsOK");

                $this->db->select("a.*, NewLoc, CountLoc, ModifLoc, tipoRsva, d.Monto, gpoTipoRsva", FALSE)
                    ->from("locs a")
                    ->join("dep_asesores b", "a.asesor = b.asesor AND a.Fecha = b.Fecha", "left", FALSE)
                    ->join("config_tipoRsva c", "IF(a.tipo IS NULL OR a.tipo='',0, a.tipo) = c.tipo
                                                AND IF(b.dep IS NULL,
                                                IF(a.asesor = - 1, - 1, 0),
                                                IF(b.dep NOT IN (0 , 3, 5, 29, 35, 50, 52),
                                                    0,
                                                    b.dep)) = c.dep", "left", FALSE)
                    ->join("locsCount d", "a.Localizador = d.Localizador AND a.Fecha=d.Fecha", "left", FALSE);
                $locsOK = $this->db->get_compiled_select();
                $this->db->query("CREATE TEMPORARY TABLE locsOK $locsOK");
            // =================================================
            // END LOCS SUMMARY
            // =================================================

            // =================================================
            // START SERVICES SUMMARY
            // =================================================
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS servicios");

                $this->db->select('a.*, gpoCanalKpi')
                    ->from("t_hoteles_test a")
                    ->join("chanGroups b", "a.chanId = b.id", "left")
                    // Definir canal
                    // ->where('gpoCanalKpi', 'PDV')
                    ->where("marca = @marca
                                AND (Fecha = @fecha
                                OR Fecha = ADDDATE(@fecha, - 364)
                                OR Fecha = ADDDATE(@fecha, - 7)
                                OR Fecha = ADDDATE(@fecha, - 1)) AND Hora <= @hora ", "", FALSE);

                if( $data['marca'] == 'Marcas Propias' ){
                    $this->db->where("pais", "@pais", FALSE);
                }

                $tHot = $this->db->get_compiled_select();

                $this->db->query("DROP TEMPORARY TABLE IF EXISTS tHot");
                $this->db->query("CREATE TEMPORARY TABLE tHot $tHot");
                $this->db->query("ALTER TABLE tHot ADD PRIMARY KEY (`Localizador`, `Fecha`, `item`, `Hora`)");

                if( $data['paq'] == 1 ){
                    $this->db->select("i.servicio");
                }else{
                    $this->db->select("IF(isPaq = 0, i.servicio, 'Paquete') as servicio", FALSE);
                }

                $this->db->select("h.*, NewLoc, CountLoc, ModifLoc, tipoRsva, d.Monto, gpoTipoRsva, j.displayNameShort as branch, cityForListing as city, FindSuperDayPDV(@fecha, p.id, 2) as super")
                    ->from("tHot h")
                    ->join("locsCount d", "h.Localizador = d.Localizador AND h.Fecha=d.Fecha", "left", FALSE)
                    ->join("dep_asesores b", "d.asesor = b.asesor AND h.Fecha = b.Fecha", "left", FALSE)
                    ->join("config_tipoRsva c", "IF(d.tipo IS NULL OR d.tipo='',0, d.tipo) = c.tipo
                                                AND IF(b.dep IS NULL,
                                                IF(d.asesor = - 1, - 1, 0),
                                                IF(b.dep NOT IN (0 , 3, 5, 29, 35, 50, 52),
                                                    0,
                                                    b.dep)) = c.dep", "left", FALSE)
                    ->join("itemTypes i", "h.itemType = i.type AND h.categoryId = i.category", "left", FALSE)
                    ->join("cat_branch j", "h.branchid = j.branchId", "left")
                    ->join("PDVs p", "h.branchid = p.branchId", "left")
                    ->having("Localizador IS NOT ", "NULL", FALSE);


                $servicios = $this->db->get_compiled_select();
                $this->db->query("CREATE TEMPORARY TABLE servicios $servicios");
            // =================================================
            // END SERVICES SUMMARY
            // =================================================
            
            
            // =================================================
            // START All PDV LISTED
            // =================================================
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS pdvList");
                $this->db->query("CREATE TEMPORARY TABLE pdvList SELECT DISTINCT
                    a.oficina,
                    displayNameShort as PdvName,
                    FINDSUPERDAYPDV(CURDATE(), a.oficina, 2) AS PdvSupervisor,
                    NOMBREASESOR(c.asesor, 1) AS PdvAsesor,
                    b.branchid
                FROM
                    asesores_plazas a
                        LEFT JOIN
                    PDVs b ON a.oficina = b.id
                        LEFT JOIN
                    dep_asesores c ON a.oficina = c.oficina
                        AND c.Fecha = CURDATE()
                        AND c.vacante IS NOT NULL
                        AND c.dep = 29
                        LEFT JOIN
                    cat_branch d ON b.branchid = d.branchid
                WHERE
                    departamento = 29 AND a.Activo = 1
                        AND Status = 1
                        AND b.branchid IS NOT NULL
                        AND a.oficina != 137");
                $this->db->query("ALTER TABLE pdvList ADD PRIMARY KEY (oficina, PdvAsesor(50))");
            // =================================================
            // END All PDV LISTED
            // =================================================
            
            // =================================================
            // START LOCS FINAL QUERY
            // =================================================
                
                $this->db->select("Fecha, PdvName as gpoTipoRsvaOk, a.branchid, PdvSupervisor as gpoCanalKpiOK, PdvAsesor, COUNT(DISTINCT CountLoc) AS Locs, city,
                                SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) MontoAll,
                                SUM(IF(gpoCanalKpi != 'PDV', VentaMXN + OtrosIngresosMXN + EgresosMXN, 0)) as MontoNoShopMontoAll,
                                COUNT(gpoCanalKpi) as gpos,
                                gpoCanalKpi as kpi,
                                SUM(IF(CountLoc IS NOT NULL,
                                    VentaMXN + OtrosIngresosMXN + EgresosMXN,
                                    0)) AS MontoSV,
                                SUM(IF(CountLoc IS NOT NULL AND gpoCanalKpi != 'PDV',
                                    VentaMXN + OtrosIngresosMXN + EgresosMXN,
                                    0)) AS MontoNoShopMontoSV,
                                SUM(IF(NewLoc IS NULL AND Monto < 0,
                                    VentaMXN + OtrosIngresosMXN + EgresosMXN,
                                    0)) XldAll", FALSE)
                    ->from("pdvList a")
                    ->join("locsOK b", "a.branchid=b.branchid",'left')
                    ->group_by(array("Fecha", "gpoCanalKpiOK", "gpoTipoRsvaOk", 'PdvAsesor'))
                    ->order_by("branch DESC, Fecha", FALSE);


                if( $l = $this->db->get() ){

                    if( $data['marca'] == 'Marcas Propias' ){
                        $this->db->select("Fecha, super as gpoCanalKpiOK, branch as gpoTipoRsvaOk, servicio", FALSE);
                    }else{
                        $this->db->select("Fecha, IF(gpoCanalKpi = 'Afiliados', gpoCanalKpi, 'Afiliados') as gpoCanalKpiOK, IF(gpoCanalKpi = 'PT.com' AND gpoTipoRsva = 'Presencial','IN',gpoTipoRsva) as gpoTipoRsvaOk, servicio", FALSE);
                    }

                    $this->db->select("COUNT(DISTINCT CountLoc) AS Locs,
                                SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) MontoAll,
                                SUM(IF(CountLoc IS NOT NULL, VentaMXN + OtrosIngresosMXN + EgresosMXN,
                                    0)) AS MontoSV,
                                SUM(IF(NewLoc IS NULL AND Monto < 0,
                                    VentaMXN + OtrosIngresosMXN + EgresosMXN,
                                    0)) XldAll,
                                SUM(IF(NewLoc IS NOT NULL, clientNights,0)) as newRN,
                                SUM(clientNights) as allRN", FALSE)
                    ->from("servicios")
                    ->group_by(array("Fecha", "gpoCanalKpiOK", "gpoTipoRsvaOk", "servicio"))

                        ->order_by("servicio, gpoCanalKpiOk, Fecha", FALSE);


                    if( $s = $this->db->get() ){
                        $lu = $this->db->query("SELECT MAX(Last_Update) as lu FROM d_Locs WHERE Fecha=CURDATE()");
                        $LU = $lu->row_array();
                        okResponse( 'Data obtenida', 'data', array( 'locs' => $l->result_array(), 'servicios' => $s->result_array()), $this, 'lu', $LU['lu'] );
                    }else{
                        errResponse('Error al compilar información servicios', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
                    }
                }else{
                    errResponse('Error al compilar información locs', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
                }
            // =================================================
            // END LOCS FINAL QUERY
            // =================================================

        });

        jsonPrint( $result );

    }

    public function getSup_get(){
        $q = $this->db->query("SELECT 
                            FINDSUPERDAYPDV(CURDATE(), oficina, 2) AS sup
                        FROM
                            dep_asesores
                        WHERE
                            Fecha = CURDATE() AND asesor = ".$_GET['usid']);
        
        okResponse('Supervisor obtenido', 'data', $q->row_array(), $this);
    }

}
