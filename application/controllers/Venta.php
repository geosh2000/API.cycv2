<?php
//defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
date_default_timezone_set('America/Mexico_city');
// use REST_Controller;


class Venta extends REST_Controller {

    public function __construct(){

        parent::__construct();
        $this->load->helper('json_utilities');
        $this->load->helper('base_venta');
        $this->load->helper('validators');
        $this->load->helper('jwt');
        $this->load->database();
    }

     
    private function locs( $soloVenta, $td, $inicio, $fin, $mp, $pais ){
        // $data['pais'] == 'MX' ? null : [$data['pais']]
        $tbl = $td ? 'baseTD' : 'baseYD';

        if( !$td ){
            $dates = $this->db->query("SELECT ADDDATE('$inicio',-364) as inicio, ADDDATE('$fin', -364) as fin");
            $dt = $dates->row_array();
            $inicio = $dt['inicio'];
            $fin = $dt['fin'];
        }

        venta_help::base($this, $inicio, $fin, false, $pais, $mp, true, false);

        $this->db->select("Fecha, tipoRsva, 
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
                ->from('base a')
                ->join("config_tipoRsva d", "IF(a.dep IS NULL, IF(a.asesor = - 1, - 1, 0), IF(a.dep NOT IN (0 , 3, 5, 29, 35, 50, 52), 0, a.dep)) = d.dep
                                            AND IF(a.tipo IS NULL OR a.tipo='',0, a.tipo) = d.tipo", "left", FALSE)
                ->group_by(array("a.Fecha", "H", "gpoTipoRsvaOk"))
                ->order_by("a.Fecha", "H");
        
        if( $soloVenta == 1 ){
            $this->db->select("SUM(IF(newLoc IS NOT NULL OR VentaMXN+OtrosIngresosMXN+EgresosMXN > 0,VentaMXN+OtrosIngresosMXN+EgresosMXN,0)) as Monto", FALSE);
        }else{
            $this->db->select("SUM(VentaMXN+OtrosIngresosMXN+EgresosMXN) as Monto", FALSE);
        }

        $query = $this->db->get_compiled_select();

        $this->db->query("DROP TEMPORARY TABLE IF EXISTS $tbl");
        $this->db->query("CREATE TEMPORARY TABLE $tbl $query");
        $this->db->query("ALTER TABLE $tbl ADD PRIMARY KEY(Fecha, H, gpoTipoRsvaOk)");
        
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
              ->from("locs a")
              ->join("dep_asesores c", "a.asesor = c.asesor AND a.Fecha = c.Fecha", "left")
              ->join("config_tipoRsva d", "IF(c.dep IS NULL, IF(a.asesor = - 1, - 1, 0), IF(c.dep NOT IN (0 , 3, 5, 29, 35, 50, 52), 0, c.dep)) = d.dep
                                            AND IF(a.tipo IS NULL OR a.tipo='',0, a.tipo) = d.tipo", "left", FALSE)
              ->group_by(array("a.Fecha", "H", "gpoTipoRsvaOk"))
              ->order_by("a.Fecha", "H");

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
                $pais = $this->uri->segment(13);
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

                if( $mp ){
                    if( !isset($pais) ){
                        $pais = 'MX';
                    }
                }else{
                    if( !isset($pais) ){
                        $pais = null;
                    }
                }

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
            if($sv == 1){
                $qSV = "SUM(IF((NewLoc IS NULL AND Monto > 0)
                            OR Monto > 0,
                        Monto,
                        0)) AS Monto";
            }else{
                $qSV = "SUM(Monto) as Monto";
            }

            $table = venta_help::base($this, $start, $end, $prod, $pais, $mp, false, false);

            // okResponse('ok', 'query', $table, $this);

            if($t){
                $pdvType = "CASE 
                WHEN gpoCanalKpi = 'PDV' THEN CASE WHEN gpoCanalKpi = 'PDV' THEN 'PDV Presencial' END
                WHEN tipoRsva LIKE '%Tag%' THEN 'CC OUT'
                WHEN tipoRsva LIKE '%Out' THEN 'CC OUT'
                WHEN tipoRsva LIKE '%IN' THEN 
                    CASE WHEN cc IS NOT NULL THEN 'Mixcoac'
                    WHEN tipoRsva LIKE '%PDV%' THEN 'PDV IN'
                    ELSE 'CC IN' END
                WHEN tipoRsva LIKE '%Presencial%' THEN 'PDV IN'
                ELSE 'Online' 
            END gpoInterno";
            }else{
                $pdvType = "CASE 
                WHEN gpoCanalKpi = 'PDV' THEN CASE WHEN a.tipo = 1 THEN 'CC OUT'
                WHEN a.tipo = 2 THEN 'PDV IN'
                ELSE 'PDV Presencial' END
                WHEN tipoRsva LIKE '%Tag%' THEN 'CC OUT'
                WHEN tipoRsva LIKE '%Out' THEN 'CC OUT'
                WHEN tipoRsva LIKE '%IN' THEN 
                    CASE WHEN cc IS NOT NULL THEN 'Mixcoac'
                    WHEN tipoRsva LIKE '%PDV%' THEN 'PDV IN'
                    ELSE 'CC IN' END
                WHEN tipoRsva LIKE '%Presencial%' THEN 'PDV Presencial'
                ELSE 'Online' 
            END gpoInterno";
            }

            if( $pais == 'CO' ){
                $curr = 'COP';
            }else{
                $curr = 'MXN';
            }

            

            $this->db->select("Fecha, $pdvType, SUM(Venta$curr+OtrosIngresos$curr+Egresos$curr) as Monto, NewLoc", FALSE)
                        ->select('servicio as producto', FALSE)
                        ->from("base a")
                        ->join("config_tipoRsva tp", "IF(a.dep IS NULL,
                                IF(a.asesor = - 1, - 1, 0),
                                IF(a.dep NOT IN (0 , 3, 5, 29, 35, 50, 52),
                                    0,
                                    IF(a.dep = 29 AND cc IS NOT NULL,
                                        35,
                                        a.dep))) = tp.dep
                                AND IF(a.tipo IS NULL OR a.tipo = '',
                                0,
                                a.tipo) = tp.tipo", 'left', FALSE)
                        ->join("itemTypes it", "a.itemType = it.type AND a.categoryId = it.category", "left")
                        ->group_by('Fecha, Localizador, item');
            $okQ = $this->db->get_compiled_select();

            $this->db->query("DROP TEMPORARY TABLE IF EXISTS tmpItems");
            $this->db->query("CREATE TEMPORARY TABLE tmpItems $okQ");

            $this->db->select("Fecha,
                                gpoInterno,
                                COUNT(DISTINCT NewLoc) AS Localizadores", FALSE)
                    ->select($qSV, FALSE)
                    ->from("tmpItems a")
                    ->group_by('Fecha, gpoInterno');

                        

            if($prod){
                $this->db->select('producto', FALSE)
                        ->group_by('producto');
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
                // okResponse('ok', 'query', $result, $this);
                
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
                            $dataLocs[$info['Fecha']][$info['h']][$info['gpoInterno']]=intVal($info['Localizadores']);
                            break;
                        case 'd':
                            $dataRes[$info['Fecha']][$info['gpoInterno']]=floatVal($monto);
                            $dataLocs[$info['Fecha']][$info['gpoInterno']]=intVal($info['Localizadores']);
                            break;
                        case 'ph':
                            $dataRes[$info['Fecha']][$info['h']][$info['producto']][$info['gpoInterno']]=floatVal($monto);
                            $dataLocs[$info['Fecha']][$info['h']][$info['producto']][$info['gpoInterno']]=intVal($info['Localizadores']);
                            break;
                        case 'pd':
                            $dataRes[$info['Fecha']][$info['producto']][$info['gpoInterno']]=floatVal($monto);
                            $dataLocs[$info['Fecha']][$info['producto']][$info['gpoInterno']]=intVal($info['Localizadores']);
                            break;
                    }

                }

                $luQ = $this->db->query("SELECT MAX(Last_Update) as lu, '$curr' as currency FROM d_Locs WHERE Fecha=CURDATE()");
                $luR = $luQ->row_array();
                $lu = $luR['lu'];
                $currency = $luR['currency'];

                okResponse( 'Data obtenida', 'data', array('venta' => $dataRes, 'locs' => $dataLocs, 'currency' => $currency), $this, 'lu', $lu );
            }else{
                errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;


        });

        jsonPrint( $result );

    }

    public function getVentaPorPDV_get(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            // ======================================================================
            // START Get Inputs
            // ======================================================================
                $start = $this->uri->segment(3);
                $end = $this->uri->segment(4);
                $sv = $this->uri->segment(5);
                $pq = $this->uri->segment(6);
                $asesor = $this->uri->segment(7);
                $total = $this->uri->segment(8);
            // ======================================================================
            // END Get Inputs
            // ======================================================================
                
            // ======================================================================
            // START Venta Query
            // ======================================================================
            if($sv == 1){
                $qSV = "COALESCE(SUM(IF((NewLoc IS NULL AND Monto > 0)
                            OR Monto > 0,
                        Monto,
                        0)),0) AS Monto,
                        COALESCE(SUM(IF(producto = 'Paquete' AND ((NewLoc IS NULL AND Monto > 0)
                            OR Monto > 0), Monto, 0)),0) as MontoPaquete,
                        COALESCE(SUM(IF(producto = 'Hotel' AND ((NewLoc IS NULL AND Monto > 0)
                            OR Monto > 0), Monto, 0)),0) as MontoHotel,
                        COALESCE(SUM(IF(producto = 'Vuelo' AND ((NewLoc IS NULL AND Monto > 0)
                            OR Monto > 0), Monto, 0)),0) as MontoVuelo,
                        COALESCE(SUM(IF(producto = 'Otros' AND ((NewLoc IS NULL AND Monto > 0)
                            OR Monto > 0), Monto, 0)),0) as MontoOtros,
                        COALESCE(SUM(IF(((NewLoc IS NULL AND Monto > 0)
                            OR Monto > 0) AND gpoCanalKpi = 'PDV',
                        Monto,
                        0)),0) AS MontoShop,
                        COALESCE(SUM(IF(((NewLoc IS NULL AND Monto > 0)
                        OR Monto > 0) AND gpoCanalKpi != 'PDV',
                        Monto,
                        0)),0) AS MontoOtrosCanales";
            }else{
                $qSV = "COALESCE(SUM(Monto),0) as Monto,
                        COALESCE(SUM(IF(producto = 'Paquete', Monto, 0)),0) as MontoPaquete,
                        COALESCE(SUM(IF(producto = 'Hotel', Monto, 0)),0) as MontoHotel,
                        COALESCE(SUM(IF(producto = 'Vuelo', Monto, 0)),0) as MontoVuelo,
                        COALESCE(SUM(IF(producto = 'Otros', Monto, 0)),0) as MontoOtros,
                        COALESCE(SUM(IF(gpoCanalKpi = 'PDV', Monto, 0)),0) as MontoShop,
                        COALESCE(SUM(IF(gpoCanalKpi != 'PDV', Monto, 0)),0) as MontoOtrosCanales";
            }

            $table = venta_help::base($this, $start, $end, true, 'MX', true, false, false);

            if( $pq == 'true' ){
                $this->db->select("IF(itemLocatorIdParent != '', 'Paquete', 
                                CASE 
                                    WHEN servicio = 'Hotel' THEN servicio 
                                    WHEN servicio = 'Vuelo' THEN servicio
                                    ELSE 'Otros'
                                END) as producto", FALSE);
            }else{
                $this->db->select("CASE 
                                    WHEN servicio = 'Hotel' THEN servicio 
                                    WHEN servicio = 'Vuelo' THEN servicio
                                    ELSE 'Otros'
                                END as producto", FALSE);
            }

            $this->db->select("Fecha, branchid, SUM(VentaMXN+OtrosIngresosMXN+EgresosMXN) as Monto, NewLoc, gpoCanalKpi, asesor", FALSE)
                        ->from("base a")
                        ->join("itemTypes it", "a.itemType = it.type AND a.categoryId = it.category", "left")
                        ->group_by('Fecha, branchid, Localizador, item');
            
            if( $asesor == 1 ){
                $this->db->where('dep', 29);
            }
            $okQ = $this->db->get_compiled_select();

            $this->db->query("DROP TEMPORARY TABLE IF EXISTS tmpItems");
            $this->db->query("CREATE TEMPORARY TABLE tmpItems $okQ");

            $this->db->select("a.Fecha as FechaQ, branchid as branchidQ, asesor as asesorQ")
                    ->select("COALESCE(COUNT(DISTINCT NewLoc),0) AS Localizadores,
                                COALESCE(COUNT(DISTINCT CASE WHEN producto = 'Hotel' THEN NewLoc END),0) as LocalizadoresHotel,
                                COALESCE(COUNT(DISTINCT CASE WHEN producto = 'Vuelo' THEN NewLoc END),0) as LocalizadoresVuelo,
                                COALESCE(COUNT(DISTINCT CASE WHEN producto = 'Paquete' THEN NewLoc END),0) as LocalizadoresPaquete,
                                COALESCE(COUNT(DISTINCT CASE WHEN producto = 'Otros' THEN NewLoc END),0) as LocalizadoresOtros", FALSE)
                    ->select($qSV, FALSE)
                    ->from("tmpItems a");
                    
            if( $asesor == 1 ){
                $this->db->group_by('asesor');
            }else{
                $this->db->group_by('branchidQ');
            }

            if( $total != 1 ){ 
                $this->db->group_by('a.Fecha'); 
            }

            if($query = $this->db->get_compiled_select()){

                if($asesor == 1){
                    $this->db->select("a.Fecha, asesor, NOMBREASESOR(asesor,2) as Nombre, TRIM(m.Ciudad) as Ciudad, FINDSUPERDAYPDV(a.Fecha, b.oficina, 1) as Supervisor", FALSE)
                            ->from("Fechas a")
                            ->join("dep_asesores b", "a.Fecha=b.Fecha", "left")
                            ->join("asesores_plazas p", "b.vacante=p.id", "left")
                            ->join("cat_zones m", "p.ciudad = m.id", "left")
                            ->where('a.Fecha BETWEEN ', "'$start' AND '$end'", FALSE)
                            ->where('dep', 29)
                            ->where('vacante IS NOT ', "NULL", FALSE);
                }else{
                    $this->db->select("Fecha, b.branchId, TRIM(cityForListing) as Ciudad, PDV, TRIM(displayNameShort) as PdvName, FINDSUPERDAYPDV(a.Fecha, b.id, 1) as Supervisor", FALSE)
                            ->from("Fechas a")
                            ->join('PDVs b', '1=1')
                            ->join('cat_zones z', 'b.branchZoneId = z.id')
                            ->where('Fecha BETWEEN ', "'$start' AND '$end'", FALSE)
                            ->where('b.Activo', 1)
                            ->where('z.pais', "MX")
                            ->where('displayNameShort !=', "home");
                }

                $join = "a.Fecha = FechaQ AND";
                $order = "Fecha, ";
                $totDisp = 0;

                if( $total == 1 ){
                    if( $asesor == 1 ){
                        $this->db->group_by('asesor');
                    }else{
                        $this->db->group_by('b.branchId');
                    }

                    $join = "";
                    $order = "";
                    $totDisp = 1;
                }

                $all = $this->db->get_compiled_select();

                $this->db->query("DROP TEMPORARY TABLE IF EXISTS q");
                $this->db->query("CREATE TEMPORARY TABLE q $query");
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS f");
                $this->db->query("CREATE TEMPORARY TABLE f $all");

                $this->db->select("*, $totDisp as Total")
                        ->from('f a');

                if($asesor == 1){
                    $this->db->join('q b', "$join a.asesor = asesorQ", 'left')
                    ->order_by("$order Nombre");
                }else{
                    $this->db->join('q b', "$join a.branchId = branchidQ", 'left')
                    ->order_by("$order PdvName");
                }
                
                if($q = $this->db->get()){
                    $result = $q->result_array();

                    $luQ = $this->db->query("SELECT MAX(Last_Update) as lu FROM d_Locs WHERE Fecha=CURDATE()");
                    $luR = $luQ->row_array();
                    $lu = $luR['lu'];

                    okResponse( 'Data obtenida', 'data', $result, $this, 'lu', $lu );
                }else{
                    errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
                }

                
            }else{
                errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

            return true;


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

        $this->locs( $data['soloVenta'] ? 1 : 0, true, $data['start'], $data['end'], $data['marca'] == 'Marcas Propias' ? true : false, $data['pais'] == 'MX' ? null : array($data['pais']) );       
        $this->locs( $data['soloVenta'] ? 1 : 0, false, $data['start'], $data['end'], $data['marca'] == 'Marcas Propias' ? true : false, $data['pais'] == 'MX' ? null : array($data['pais']) );
        

        $this->db->query("DROP TEMPORARY TABLE IF EXISTS locs");

        if( $this->db->query("CREATE TEMPORARY TABLE xHora SELECT * FROM baseTD UNION SELECT * FROM baseYD") ){
                
            $this->db->query("ALTER TABLE xHora ADD PRIMARY KEY(Fecha, H, gpoTipoRsvaOk)");
                
            $q = $this->db->get('xHora');
            $l = $this->db->query("SELECT MAX(Last_Update) as lu FROM t_hoteles_test WHERE Fecha>=ADDDATE(CURDATE(),-1)");
                
                
            okResponse( 'Data obtenida', 'data', $q->result_array(), $this, 'lu', $l->row_array() );
        }else{
            errResponse('Error al compilar información final', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
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
                $qD = $this->db->query("SELECT CAST('".$data['Fecha']."' as DATE) as td, ADDDATE(CAST('".$data['Fecha']."' as DATE),-1) as yd, ADDDATE(CAST('".$data['Fecha']."' as DATE),-7) as lw, ADDDATE(CAST('".$data['Fecha']."' as DATE),-364) as ly");
                $days = $qD->row_array();

                $mp = $data['marca'] == 'Marcas Propias' ? true : false;
                $pais = $data['marca'] == 'Marcas Propias' && $data['pais'] == 'MX' ? null : $data['pais'];

                if( $mp == true && $pais == 'CO' ){
                    $currency = 'COP';
                }else{
                    $currency = 'MXN';
                }
            // =================================================
            // END SET PARAMS FOR QUERY
            // =================================================

            // =================================================
            // START Base QUERY
            // =================================================

                $queryDate = array();

                foreach($days as $day => $date){
                    venta_help::base($this, $date, $date, true, $pais, $mp, true, false);
                    $this->db->select('*')->from('base');

                    // Define hour for historic views
                    if( $data['h'] != 1 ){
                        $this->db->where('Hora <=', "CAST('".$data['Hora']."' as TIME)", FALSE);
                    }

                    $tmpQuery = $this->db->get_compiled_select();

                    $this->db->query("DROP TEMPORARY TABLE IF EXISTS $day");
                    $this->db->query("CREATE TEMPORARY TABLE $day $tmpQuery");
                }

                $this->db->query("CREATE TEMPORARY TABLE baseOK SELECT * FROM td UNION SELECT * FROM yd UNION SELECT * FROM lw UNION SELECT * FROM ly");
                $this->db->query("ALTER TABLE baseOK ADD PRIMARY KEY (Fecha, Hora, Localizador, item)");
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS td");
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS lw");
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS ly");
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS base");  
            
            // =================================================
            // END Base QUERY
            // =================================================
                
                
            // =================================================
            // START LOCS QUERY
            // =================================================
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS locsCount");
                $this->db->select("Fecha, Localizador, asesor, dep, SUM(Venta".$currency."+OtrosIngresos".$currency."+Egresos".$currency.") as Venta, NewLoc, gpoCanalKpi, tipo")
                    ->from("baseOK")
                    ->group_by(array('Fecha','Localizador'));
                
                $locs = $this->db->get_compiled_select();
                $this->db->query("CREATE TEMPORARY TABLE locsCount $locs");
                $this->db->query("ALTER TABLE locsCount ADD PRIMARY KEY (`Localizador`, `Fecha`)");
                    
            // =================================================
            // END LOCS QUERY
            // =================================================
                
            // =================================================
            // START Locs Summary
            // =================================================
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS locsOK");
                
                $this->db->select("a.*, tipoRsva, gpoTipoRsva", FALSE)
                    ->from("locsCount a")
                    ->join("config_tipoRsva c", "IF(a.tipo IS NULL OR a.tipo='',0, a.tipo) = c.tipo
                                                AND IF(a.dep IS NULL,
                                                IF(a.asesor = - 1, - 1, 0),
                                                IF(a.dep NOT IN (0 , 3, 5, 29, 35, 50, 52),
                                                    0,
                                                    a.dep)) = c.dep", "left", FALSE);
                $locsOK = $this->db->get_compiled_select();
                $this->db->query("CREATE TEMPORARY TABLE locsOK $locsOK");
            // =================================================
            // END Locs Summary
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

                $this->db->select("Fecha, Localizador, item, asesor, a.dep, SUM(Venta".$currency."+OtrosIngresos".$currency."+Egresos".$currency.") as Venta, NewLoc, gpoCanalKpi, a.tipo, tipoRsva, gpoTipoRsva, clientNights")
                    ->from("baseOK a")
                    ->join("config_tipoRsva c", "IF(a.tipo IS NULL OR a.tipo='',0, a.tipo) = c.tipo
                                                AND IF(a.dep IS NULL,
                                                IF(a.asesor = - 1, - 1, 0),
                                                IF(a.dep NOT IN (0 , 3, 5, 29, 35, 50, 52),
                                                    0,
                                                    a.dep)) = c.dep", "left", FALSE)
                    ->join("itemTypes i", "a.itemType = i.type AND a.categoryId = i.category", "left", FALSE)
                    ->group_by(array('Fecha','Localizador', 'item'));

                $servicios = $this->db->get_compiled_select();
                $this->db->query("CREATE TEMPORARY TABLE servicios $servicios");
                $this->db->query("ALTER TABLE servicios ADD PRIMARY KEY (Fecha, Localizador, item)");

            // =================================================
            // END SERVICIOS SUMMARY
            // =================================================
                
                
                if( $mp ){
                $this->db->select("Fecha, gpoCanalKpi as gpoCanalKpiOK, IF(gpoCanalKpi = 'PT.com' AND gpoTipoRsva = 'Presencial','In',gpoTipoRsva) as gpoTipoRsvaOk", FALSE);
                }else{
                $this->db->select("Fecha, IF(gpoCanalKpi = 'Afiliados', gpoCanalKpi, 'Afiliados') as gpoCanalKpiOK, IF(gpoCanalKpi = 'PT.com' AND gpoTipoRsva = 'Presencial','In',gpoTipoRsva) as gpoTipoRsvaOk", FALSE);
                }
                
                
                $this->db->select("COUNT(DISTINCT NewLoc) AS Locs,
                                SUM(Venta) MontoAll,
                                SUM(IF((NewLoc IS NULL AND Venta > 0)
                                    OR Venta > 0, Venta, 0)) as
                                MontoSV,
                                SUM(IF(NewLoc IS NULL AND Venta < 0,
                                    Venta,
                                    0)) as XldAll", FALSE)
                    ->from("locsOK")
                    ->group_by(array("Fecha", "gpoCanalKpiOK", "gpoTipoRsvaOk"))
                    ->order_by("gpoCanalKpiOk DESC, Fecha", FALSE);
                    
                
                if( $l = $this->db->get() ){
                    
                    if( $data['marca'] == 'Marcas Propias' ){
                    $this->db->select("Fecha, gpoCanalKpi as gpoCanalKpiOK, IF(gpoCanalKpi = 'PT.com' AND gpoTipoRsva = 'Presencial','In',gpoTipoRsva) as gpoTipoRsvaOk, servicio", FALSE);
                    }else{
                    $this->db->select("Fecha, IF(gpoCanalKpi = 'Afiliados', gpoCanalKpi, 'Afiliados') as gpoCanalKpiOK, IF(gpoCanalKpi = 'PT.com' AND gpoTipoRsva = 'Presencial','IN',gpoTipoRsva) as gpoTipoRsvaOk, servicio", FALSE);
                    }
                    
                    $this->db->select("COUNT(DISTINCT NewLoc) AS Locs,
                                SUM(Venta) MontoAll,
                                SUM(IF((NewLoc IS NULL AND Venta > 0)
                                    OR Venta > 0, Venta, 0)) as
                                MontoSV,
                                SUM(IF(NewLoc IS NULL AND Venta < 0,
                                    Venta,
                                    0)) as XldAll,
                                    SUM(clientNights) as newRN,
                                SUM(clientNights) as allRN", FALSE)
                    ->from("servicios")
                    ->group_by(array("Fecha", "gpoCanalKpiOK", "gpoTipoRsvaOk", "servicio"))
                        ->order_by("servicio, gpoCanalKpiOk DESC, Fecha", FALSE);
                    
                    
                    if( $s = $this->db->get() ){
                        $lu = $this->db->query("SELECT MAX(Last_Update) as lu FROM t_hoteles_test WHERE Fecha='".$days['td']."'");
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
            // START GET PARAMS
            // =================================================
            $data = $this->put();
            // =================================================
            // END GET PARAMS
            // =================================================

            // =================================================
            // START SET PARAMS FOR QUERY
            // =================================================
                $qD = $this->db->query("SELECT CAST('".$data['Fecha']."' as DATE) as td, ADDDATE(CAST('".$data['Fecha']."' as DATE),-1) as yd, ADDDATE(CAST('".$data['Fecha']."' as DATE),-7) as lw, ADDDATE(CAST('".$data['Fecha']."' as DATE),-364) as ly");
                $days = $qD->row_array();

                $mp = $data['marca'] == 'Marcas Propias' ? true : false;
                $pais = $data['marca'] == 'Marcas Propias' && $data['pais'] == 'MX' ? null : $data['pais'];
            // =================================================
            // END SET PARAMS FOR QUERY
            // =================================================

            // =================================================
            // START Base QUERY
            // =================================================

                $queryDate = array();

                foreach($days as $day => $date){
                    venta_help::base($this, $date, $date, true, $pais, $mp, true, false);
                    $this->db->select('*')->from('base');

                    // Define hour for historic views
                    if( $data['h'] != 1 ){
                        $this->db->where('Hora <=', "CAST('".$data['Hora']."' as TIME)", FALSE);
                    }

                    $tmpQuery = $this->db->get_compiled_select();

                    $this->db->query("DROP TEMPORARY TABLE IF EXISTS $day");
                    $this->db->query("CREATE TEMPORARY TABLE $day $tmpQuery");
                }

                $this->db->query("CREATE TEMPORARY TABLE baseOK SELECT * FROM td UNION SELECT * FROM yd UNION SELECT * FROM lw UNION SELECT * FROM ly");
                $this->db->query("ALTER TABLE baseOK ADD PRIMARY KEY (Fecha, Hora, Localizador, item)");
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS td");
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS lw");
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS ly");
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS base");  
            
            // =================================================
            // END Base QUERY
            // =================================================
                
                
            // =================================================
            // START LOCS QUERY
            // =================================================
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS locsCount");
                $this->db->select("Fecha, Localizador, asesor, dep, SUM(VentaMXN+OtrosIngresosMXN+EgresosMXN) as Venta, NewLoc, gpoCanalKpi, a.tipo, c.displayNameShort as branch, cityForListing as city, FindSuperDayPDV(@fecha, c.id, 2) as super, a.branchid")
                    ->from("baseOK a")
                    ->join("PDVs c", "a.branchid = c.branchId", "left")
                    ->group_by(array('Fecha','Localizador'));
                
                $locs = $this->db->get_compiled_select();
                $this->db->query("CREATE TEMPORARY TABLE locsCount $locs");
                $this->db->query("ALTER TABLE locsCount ADD PRIMARY KEY (`Localizador`, `Fecha`)");
                    
            // =================================================
            // END LOCS QUERY
            // =================================================

            // =================================================
            // START Locs Summary
            // =================================================
                $this->db->query("DROP TEMPORARY TABLE IF EXISTS locsOK");
                    
                $this->db->select("a.*, tipoRsva, gpoTipoRsva", FALSE)
                    ->from("locsCount a")
                    ->join("config_tipoRsva c", "IF(a.tipo IS NULL OR a.tipo='',0, a.tipo) = c.tipo
                                                AND IF(a.dep IS NULL,
                                                IF(a.asesor = - 1, - 1, 0),
                                                IF(a.dep NOT IN (0 , 3, 5, 29, 35, 50, 52),
                                                    0,
                                                    a.dep)) = c.dep", "left", FALSE);
                $locsOK = $this->db->get_compiled_select();
                $this->db->query("CREATE TEMPORARY TABLE locsOK $locsOK");
            // =================================================
            // END Locs Summary
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

                $this->db->select("Fecha, Localizador, item, asesor, a.dep, SUM(VentaMXN+OtrosIngresosMXN+EgresosMXN) as Venta, NewLoc, gpoCanalKpi, a.tipo, tipoRsva, gpoTipoRsva, clientNights, j.displayNameShort as branch, cityForListing as city, FindSuperDayPDV(@fecha, j.id, 2) as super, a.branchid")
                    ->from("baseOK a")
                    ->join("config_tipoRsva c", "IF(a.tipo IS NULL OR a.tipo='',0, a.tipo) = c.tipo
                                                AND IF(a.dep IS NULL,
                                                IF(a.asesor = - 1, - 1, 0),
                                                IF(a.dep NOT IN (0 , 3, 5, 29, 35, 50, 52),
                                                    0,
                                                    a.dep)) = c.dep", "left", FALSE)
                    ->join("itemTypes i", "a.itemType = i.type AND a.categoryId = i.category", "left", FALSE)
                    ->join("PDVs j", "a.branchid = j.branchId", "left")
                    ->group_by(array('Fecha','Localizador', 'item'));

                $servicios = $this->db->get_compiled_select();
                $this->db->query("CREATE TEMPORARY TABLE servicios $servicios");
                $this->db->query("ALTER TABLE servicios ADD PRIMARY KEY (Fecha, Localizador, item)");

            // =================================================
            // END SERVICIOS SUMMARY
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

                $this->db->select("Fecha, PdvName as gpoTipoRsvaOk, a.branchid, PdvSupervisor as gpoCanalKpiOK, PdvAsesor, 
                                    COUNT(DISTINCT NewLoc) AS Locs,
                                    SUM(Venta) MontoAll,
                                    SUM(IF(gpoCanalKpi != 'PDV', Venta, 0)) as MontoNoShopMontoAll,
                                    COUNT(gpoCanalKpi) as gpos,
                                    gpoCanalKpi as kpi,
                                    SUM(IF((NewLoc IS NULL AND Venta > 0)
                                        OR Venta > 0, Venta, 0)) as MontoSV,
                                    SUM(IF(((NewLoc IS NULL AND Venta > 0)
                                        OR Venta > 0) AND gpoCanalKpi != 'PDV', Venta, 0)) as MontoNoShopMontoSV,
                                    SUM(IF(NewLoc IS NULL AND Venta < 0,
                                        Venta,
                                        0)) as XldAll", FALSE)
                        ->from("pdvList a")
                        ->join("locsOK b", "a.branchid=b.branchid",'left')
                        ->group_by(array("Fecha", "gpoCanalKpiOK", "gpoTipoRsvaOk", 'PdvAsesor'))
                        ->having('gpoCanalKpiOK IS NOT NULL', NULL, FALSE)
                        ->order_by("branch DESC, Fecha", FALSE);

                if( $l = $this->db->get() ){

                            $this->db->select("Fecha, PdvName as gpoTipoRsvaOk, a.branchid, PdvSupervisor as gpoCanalKpiOK, PdvAsesor, servicio,
                                        COUNT(DISTINCT NewLoc) AS Locs,
                                        SUM(Venta) MontoAll,
                                        SUM(IF(gpoCanalKpi != 'PDV', Venta, 0)) as MontoNoShopMontoAll,
                                        COUNT(gpoCanalKpi) as gpos,
                                        gpoCanalKpi as kpi,
                                        SUM(IF((NewLoc IS NULL AND Venta > 0)
                                            OR Venta > 0, Venta, 0)) as MontoSV,
                                        SUM(IF((NewLoc IS NULL AND gpoCanalKpi != 'PDV' AND Venta > 0)
                                            OR Venta > 0, Venta, 0)) as MontoNoShopMontoSV,
                                        SUM(IF(NewLoc IS NULL AND Venta < 0,
                                            Venta,
                                            0)) as XldAll,
                                        SUM(clientNights) as newRN,
                                        SUM(clientNights) as allRN", FALSE)
                            ->from("pdvList a")
                            ->join("servicios b", "a.branchid=b.branchid",'left')
                            ->group_by(array("Fecha", "gpoCanalKpiOK", "gpoTipoRsvaOk", "servicio"))
                            ->having('gpoCanalKpiOK IS NOT NULL', NULL, FALSE)
                            ->order_by("servicio, Fecha", FALSE);

                    if( $s = $this->db->get() ){
                        $lu = $this->db->query("SELECT MAX(Last_Update) as lu FROM d_Locs WHERE Fecha='".$days['td']."'");
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

    public function weekSale_get(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $pais = $this->uri->segment(3);
            $inicio = $this->uri->segment(4);
            $fin = $this->uri->segment(5);
            $porPdv = $this->uri->segment(6);

            if( !isset($fin) ){
                $fin = $inicio;
                $this->db->select('a.Fecha as FechaOk');
            }else{
                if($inicio != $fin){
                    $this->db->select("CONCAT('$inicio',' - ','$fin') as FechaOk", FALSE);
                }else{
                    $this->db->select('a.Fecha as FechaOk');
                }
            }

            $this->db->from("t_hoteles_test a")
                    ->join("chanGroups b","a.chanId = b.id","left",FALSE)
                    ->join("itemTypes it","a.itemType = it.type
                                            AND a.categoryId = it.category","left",FALSE)
                    ->join("t_masterlocators ml","a.Localizador = ml.masterlocatorid","left",FALSE)
                    ->join("dep_asesores dp","ml.asesor = dp.asesor
                                                AND a.Fecha = dp.Fecha","left",FALSE)
                    ->join("config_tipoRsva tr","IF(dp.dep IS NULL,
                                                IF(ml.asesor = - 1, - 1, 0),
                                                IF(dp.dep NOT IN (0 , 3, 5, 29, 35, 50, 52),
                                                    0,
                                                    dp.dep)) = tr.dep
                                                AND IF(ml.tipo IS NULL OR ml.tipo = '',
                                                0,
                                                ml.tipo) = tr.tipo","left",FALSE)
                    ->where(array('b.pais'=>$pais, 'marca' => 'Marcas Propias', 'a.Fecha >=' => $inicio, 'a.Fecha <=' => $fin))
                    ->where(array('Servicio'=>'Hotel', 'CAST(ml.dtCreated AS DATE) = a.Fecha' => NULL));

            if( $porPdv == 1 ){
                $this->db->select("b.pais,
                            gpoCanalKpi,
                            gpoTipoRsva,
                            Hotel,
                            hotelId,
                            placeId,
                            Destination,
                            PDV, nombreZona as Zona,
                            COUNT(DISTINCT CONCAT(Localizador,'-', item)) as MLs,
                            SUM(clientNights) as RN", FALSE)
                        ->join('PDVs p', 'a.branchId = p.branchid', 'left')
                        ->join('pdv_zonesCustom z', 'p.customZone = z.id', 'left')
                        ->group_by(array('FechaOk', 'gpoCanalKpi','gpoTipoRsva','Hotel','Destination','PDV'));
            }else{
                $this->db->select("b.pais,
                            gpoCanalKpi,
                            gpoTipoRsva,
                            Hotel,
                            hotelId,
                            placeId,
                            Destination,
                            YEAR(checkIn) AS Anio,
                            WEEK(checkIn, 4) AS Semana,
                            COUNT(DISTINCT CONCAT(Localizador,'-', item)) as MLs,
                            SUM(clientNights) as RN", FALSE)
                        ->group_by(array('FechaOk', 'gpoCanalKpi','gpoTipoRsva','Hotel','Destination','Anio','Semana'));
            }

                    $qu = $this->db->get_compiled_select();
                    
                    
            if( $q = $this->db->query($qu) ){
                okResponse('Data Obtenida', 'data', $q->result_array(), $this, "pdvGroup", $porPdv);
            }else{
                errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

        });

    }

    public function masBuscado_get(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            if( $q = $this->db->select('*')->select("STR_TO_DATE(CONCAT(anio,semana,' Monday'), '%Y%u %W') as fd", FALSE)->order_by('destino')->order_by('hotel')->get('t_masBuscados') ){
                okResponse('Data Obtenida', 'data', $q->result_array(), $this);
            }else{
                errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

        });

    }

    public function getSup_get(){
        $q = $this->db->query("SELECT 
                            FINDSUPERDAYPDV(CURDATE(), oficina, 2) AS sup
                        FROM
                            dep_asesores
                        WHERE
                            Fecha = CURDATE() AND asesor = ".$_GET['usid']." 
                        HAVING sup IS NOT NULL");
        
        okResponse('Supervisor obtenido', 'data', $q->row_array(), $this);
    }

    public function avancePdvDiario_get(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $inicio = $this->uri->segment(3);
            $fin = $this->uri->segment(4);
            
            $this->db->query("SET @inicio='$inicio'");
            $this->db->query("SET @fin='$fin'");

            $this->db->select("a.Fecha,
                            a.asesor,
                            NOMBREASESOR(a.asesor, 2) AS Nombre,
                            p.PDV,
                            zc.nombreZona,
                            FINDSUPERDAYPDV(a.Fecha, COALESCE(ap.pdv, dp.oficina), 2) AS Supervisor,
                            FINDCOORDDAYPDV(a.Fecha, zc.id, 2) AS Coordinador,
                            MontoSV AS ventaTotal,
                            meta_total_diaria / mp.asesores AS metaTotalDia,
                            MontoSV / (meta_total_diaria / mp.asesores) AS cumplimiento_metaTotalDia,
                            HotelAllInSV + HotelAllNotInSV AS ventaHotel,
                            meta_hotel_diaria / mp.asesores AS metaHotelDia,
                            (HotelAllInSV + HotelAllNotInSV) / (meta_hotel_diaria / mp.asesores) AS cumplimiento_metaHotelDia", FALSE) 
            ->from("graf_dailySale a")
            ->join("dep_asesores dp", "a.asesor = dp.asesor AND a.Fecha = dp.Fecha", "left")
            ->join("asesores_programacion ap", "a.asesor = ap.asesor AND a.Fecha = ap.Fecha", "left")
            ->join("metas_pdv mp", "COALESCE(ap.pdv, dp.oficina) = mp.pdv
                                    AND YEAR(a.Fecha) = mp.anio
                                    AND MONTH(a.Fecha) = mp.mes", "left", FALSE)
            ->join("PDVs p", "COALESCE(ap.pdv, dp.oficina) = p.id", "left")
            ->join("pdv_zonesCustom zc", "p.customZone = zc.id", "left")
            ->where("a.Fecha BETWEEN ", "@inicio AND @fin", FALSE)
            ->where("a.dep", 29)
            ->where_not_in("dp.puesto", array(11 , 17, 48))
            ->order_by(array('Fecha','Nombre'));

            if( $q = $this->db->get() ){
                okResponse('Data Obtenida', 'data', $q->result_array(), $this);
            }else{
                errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            }

        });

    }

    public function avancePdvMes_get(){

        $result = validateToken( $_GET['token'], $_GET['usn'], $func = function(){

            $mes = $this->uri->segment(3);
            $anio = $this->uri->segment(4);

            
            
            $this->db->query("SET @inicio='$anio-$mes-01'");
            $this->db->query("SET @fin=LAST_DAY(@inicio)");

            $this->db->query("DROP TEMPORARY TABLE IF EXISTS ventaPdv");

            $this->db->query("CREATE TEMPORARY TABLE ventaPdv
                                SELECT 
                                    Fecha,
                                    branchid,
                                    COALESCE(asesor,0) as asesor,
                                    SUM(VentaMXN + OtrosIngresosMXN + EgresosMXN) AS Monto,
                                    SUM(IF(CAST(dtCreated AS DATE) = a.Fecha,
                                        VentaMXN + OtrosIngresosMXN + EgresosMXN,
                                        VentaMXN + OtrosIngresosMXN)) AS MontoSV,
                                    dtCreated,
                                    COALESCE(Servicio, 'Otros') as Servicio
                                FROM
                                    t_hoteles_test a
                                        LEFT JOIN
                                    t_masterlocators ml ON a.Localizador = ml.masterlocatorid
                                        LEFT JOIN
                                    itemTypes it ON a.itemType = it.type
                                        AND a.categoryId = it.category
                                WHERE
                                    a.Fecha BETWEEN @inicio AND @fin
                                GROUP BY Fecha, branchid , COALESCE(asesor,0) , COALESCE(Servicio, 'Otros')");
            
            $this->db->query("ALTER TABLE ventaPdv ADD PRIMARY KEY (Fecha, Servicio(15), branchid, asesor)");
            $this->db->query("DROP TEMPORARY TABLE IF EXISTS byPDV");
            $this->db->query("CREATE TEMPORARY TABLE byPDV
                                SELECT 
                                    a.PDV,
                                    displayNameShort,
                                    zc.nombreZona AS Zona,
                                    FINDCOORDDAYPDV(f.Fecha, zc.id, 2) AS Coordinador,
                                    FINDSUPERDAYPDV(f.Fecha, a.id,2) AS Supervisor,
                                    SUM(COALESCE(Monto,0)) AS Monto,
                                    SUM(IF(Servicio = 'Hotel',
                                        COALESCE(Monto,0),
                                        0)) AS MontoHotel,
                                    COALESCE(meta_total_diaria,0) as meta_total_diaria,
                                    COALESCE(meta_hotel_diaria,0) as meta_hotel_diaria,
                                    COALESCE(meta_total,0) as meta_total,
                                    COALESCE(meta_hotel,0) as meta_hotel
                                FROM
                                    Fechas f
                                        JOIN
                                    PDVs a
                                        LEFT JOIN
                                    pdv_zonesCustom zc ON a.customZone = zc.id
                                        LEFT JOIN
                                    ventaPdv vp ON a.branchid = vp.branchid
                                        AND f.Fecha = vp.Fecha LEFT JOIN metas_pdv mp ON mp.mes=MONTH(@inicio) AND mp.anio=YEAR(@inicio) AND mp.pdv=a.id
                                WHERE
                                    Activo = 1 AND pais = 'MX' AND f.Fecha BETWEEN @inicio AND @fin
                                GROUP BY a.PDV");
            
            $zoneQ = "SELECT 
                                Zona,
                                Coordinador,
                                SUM(Monto) AS Monto,
                                SUM(MontoHotel) AS MontoHotel,
                                SUM(meta_total) AS meta_Zona,
                                SUM(meta_total)/DAY(@fin) AS meta_Zona_diaria,
                                SUM(meta_hotel) AS MetaHotel_Zona,
                                SUM(meta_hotel)/DAY(@fin) AS MetaHotel_Zona_diaria
                            FROM
                                byPDV
                            GROUP BY Zona";

            $superQ = "SELECT
                            Zona,
                            Supervisor,
                            SUM(Monto) AS Monto,
                            SUM(MontoHotel) AS MontoHotel,
                            SUM(meta_total) AS meta_Super,
                            SUM(meta_hotel) AS MetaHotel_Super,
                            SUM(meta_total)/DAY(@fin) AS meta_Super_diaria,
                            SUM(meta_hotel)/DAY(@fin) AS MetaHotel_Super_diaria
                        FROM
                            byPDV
                        GROUP BY Supervisor";

            $pdvQ = "SELECT
                            Zona,
                            Supervisor,
                            displayNameShort as PDV,
                            SUM(Monto) AS Monto,
                            SUM(MontoHotel) AS MontoHotel,
                            SUM(meta_total) AS meta_PDV,
                            SUM(meta_hotel) AS MetaHotel_PDV,
                            SUM(meta_total)/DAY(@fin) AS meta_PDV_diaria,
                            SUM(meta_hotel)/DAY(@fin) AS MetaHotel_PDV_diaria
                        FROM
                            byPDV
                        GROUP BY PDV";

            $this->db->query("CREATE TEMPORARY TABLE dailyPdv SELECT 
                            dp.Fecha,
                            NOMBREASESOR(dp.asesor, 2) AS Nombre,
                            NOMBREPUESTO(dp.puesto) as Puesto,
                            FINDSUPERDAYPDV(dp.Fecha, oficina, 2) AS Supervisor,
                            COALESCE(ap.pdv, dp.oficina) AS pdvId,
                            SUM(COALESCE(Monto, 0)) AS Monto,
                            SUM(IF(Servicio = 'Hotel',
                                COALESCE(Monto, 0),
                                0)) AS MontoHotel,
                            IF(js=je,0,COALESCE(meta_total_diaria/mp.asesores,0)) as metaTotal,
                            IF(js=je,0,COALESCE(meta_hotel_diaria/mp.asesores,0)) as metaHotel
                        FROM
                            dep_asesores dp
                                LEFT JOIN
                            asesores_programacion ap ON dp.asesor = ap.asesor
                                AND dp.Fecha = ap.Fecha
                                LEFT JOIN
                            ventaPdv v ON dp.asesor = v.asesor
                                AND dp.Fecha = v.Fecha
                                LEFT JOIN
                            metas_pdv mp ON COALESCE(ap.pdv, dp.oficina) = mp.pdv
                                AND MONTH(@inicio) = mp.mes
                                AND YEAR(@inicio) = mp.anio
                        WHERE
                            dp.puesto NOT IN (11 , 17, 48)
                                AND dp.Fecha BETWEEN @inicio AND @fin
                                AND dp.dep = 29
                                AND dp.vacante IS NOT NULL
                        GROUP BY dp.Fecha , dp.asesor");

            $dailyQ = "SELECT * FROM dailyPdv";
            $sumAllQ = "SELECT * FROM byPDV";
            
            $sumQ = "SELECT 
                        Nombre,
                        Puesto,
                        Supervisor,
                        SUM(COALESCE(Monto, 0)) AS Monto,
                        SUM(MontoHotel) AS MontoHotel,
                        SUM(metaTotal) AS meta_asesor,
                        SUM(metaHotel) AS MetaHotel_asesor,
                        SUM(IF(Fecha <= CURDATE(), metaTotal, 0)) AS meta_asesor_diaria,
                        SUM(IF(Fecha <= CURDATE(), metaHotel, 0)) AS MetaHotel_asesor_diaria
                    FROM
                        dailyPdv
                    GROUP BY Nombre";

            if( $zQ = $this->db->query( $zoneQ ) ){
                if( $sQ = $this->db->query( $superQ ) ){
                    if( $pQ = $this->db->query( $pdvQ ) ){
                        if( $dQ = $this->db->query( $dailyQ ) ){
                            if( $sumQ = $this->db->query( $sumQ ) ){
                                if( $sumAQ = $this->db->query( $sumAllQ ) ){

                                    okResponse('Data Obtenida', 'data', array( 'zones' => $zQ->result_array(), 
                                                                                'super' => $sQ->result_array(), 
                                                                                'pdv' => $pQ->result_array(), 
                                                                                'daily' => $dQ->result_array(), 
                                                                                'sumAll' => $sumAQ->result_array(), 
                                                                                'asesor' => $sumQ->result_array()), 
                                                $this);
                                }
                            }
                        }
                    }
                }
            }
            
            errResponse('Error al compilar información', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error());
            

        });

    }

}
