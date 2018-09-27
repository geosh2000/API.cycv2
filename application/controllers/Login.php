<?php
defined('BASEPATH') OR exit('No direct script access allowed');
require( APPPATH.'/libraries/REST_Controller.php');
// use REST_Controller;


class Login extends REST_Controller {

  public function __construct(){

    parent::__construct();
    $this->load->helper('json_utilities');
    $this->load->helper('validators');
    $this->load->helper('jwt');
    $this->load->database();
  }

  public function login_put(){

    $data = $this->put();
    $ci_response = $this->uri->segment(3);

    if(!$data['usp'] || trim($data['usp']) == ''){
      errResponse('Error de autenticacion', REST_Controller::HTTP_BAD_REQUEST, $this, 'msg', "No se recibió ningún password");
    }

    //Username validation
    $user = $this->validateUser( $data['usn'] );

    //Pswd Validation
    if( $this->noAdValidation( $user, $data['usp'] ) ){

      $credentials  = $this->getCredentials( $user['username'] );
      $hcInfo       = $this->getHcInfo( $user['username'] );
      $token        = $this->tokenAssign( $user['username'], $data['remember'] );

      $response = array(
                          'credentials' => $credentials,
                          'token'       => '',
                          'hcInfo'      => $hcInfo,
                          'token'       => $token['token'],
                          'tokenExpire' => $token['expire'],
                          'username'    => $user['username']
                        );

      if( isset($ci_response) ){
        okResponse('Login Correcto', 'data', $response, $this);
      }
      
      $this->response( $response );

    }


  }
    
  public function logout_put(){

    $data = $this->put();
      
    $this->db->where(array('asesor' => $data['asesor'], 'Fecha' => $data['Fecha']))
            ->delete('horarios_check');
      
    $this->db->set($data);

    if( $this->db->insert('horarios_check') ){
        okResponse('Logout Correcto', 'data', true, $this);
    }else{
        errResponse('Error al guardar horarios aceptados', REST_Controller::HTTP_BAD_REQUEST, $this, 'msg', "Verifica con WFM tu deslogueo correcto y confirma tus horarios para los siguientes dias");
    }

  }

  private function adCheck( $usr, $psw ){

    // conexión al servidor LDAP
    $ldapconn = ldap_connect("ccad02.pricetravel.com.mx")
      or die("No se pudo establecer conexion con el LDAP server.");

    if( $ldapconn ){

      // Autenticación
      if( $ldapbind = ldap_bind( $ldapconn, $usr, $psw ) ){
        ldap_close($ldapconn);
        return true;
      }else{
        ldap_close($ldapconn);
        errResponse('Error de autenticacion', REST_Controller::HTTP_BAD_REQUEST, $this, 'msg', "Usuario y/o contraseña incorrectos. Validación hecha por AD");
      }

    }else{
      ldap_close($ldapconn);
      errResponse('Error de conexión con server LDAP', REST_Controller::HTTP_BAD_REQUEST, $this, 'msg', "No es posible conectarse con el servidor LDAP");
    }

  }

  private function validateUser( $user ){

    // Validate Domain
    if( strpos( $user, "@" ) == "" ){

      return array(
                    "username"  => $user,
                    "mail"      => $user."@pricetravel.com.mx"
                  );

    }else{

      if( substr( $user, strpos( $user, "@" ), 15 ) == "@pricetravel.co" ){

        return array(
                      "username"  => substr( $user, 0, strpos( $user, "@" ) ),
                      "mail"      => $user
                    );

    	}else{
    		errResponse('Dominio Incorrecto', REST_Controller::HTTP_BAD_REQUEST, $this, 'msg', "Sólo los dominios de PriceTravel son aceptados. Si tienes una cuenta de usuario externo, esta no debe incluir ningún dominio.");
    	}

    }

  }

  private function noAdValidation( $usr, $psw ){

    if( $q = $this->db->query("SELECT noAD, hashed_pswd, IF(CURDATE() > Egreso OR a.active=0,0,1) as Active FROM userDB a LEFT JOIN Asesores b ON a.asesor_id = b.id WHERE username = '".$usr['username']."'") ){

      if( $q->num_rows() == 0 ){
        errResponse('Usuario Incorrecto', REST_Controller::HTTP_BAD_REQUEST, $this, 'msg', "El usuario no existe en la base de datos");
      }else{
        $noAd = $q->row_array();

        if( $noAd['Active'] != 1 ){
          errResponse('Usuario Inactivo', REST_Controller::HTTP_BAD_REQUEST, $this, 'msg', "Usuario inactivo en sistema");
        }

        if( $noAd['noAD'] == 1 ){

          // Validate password
          if( password_verify( $psw, $noAd['hashed_pswd'] ) ){
            return true;
          }else{
            errResponse('Contraseña Incorrecta', REST_Controller::HTTP_BAD_REQUEST, $this, 'msg', "Contraseña incorrecta. La validación se hace por DB");
          }

        }else{

          if( $this->adCheck( $usr['mail'], $psw ) ){
            return true;
          }

        }
      }

    }else{
      errResponse('Dominio Incorrecto', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error(), 'msg', 'Error al consultar base de usuarios de ComeyCome');
    }

  }

  private function getCredentials( $usr ){

    $q = $this->db->query("SELECT profile FROM userDB WHERE username='$usr'");
    $qData = $q->row_array();
    $profile = $qData['profile'];

    if( $q = $this->db->query("SELECT * FROM profilesDB WHERE id=$profile") ){
      return $q->row();
    }else{
      errResponse('Error de perfiles', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error(), 'msg', 'Error al consultar perfiles para usuario');
    }

  }

  private function getHcInfo( $usr ){

    if ( $q = $this->db->query("SELECT
                              a.id AS vacante,
                              d.clave AS hc_udn_clave,
                              d.id AS hc_udn,
                              c.id AS hc_area,
                              c.clave AS hc_area_clave,
                              b.clave AS hc_dep_clave,
                              b.id AS hc_dep,
                              e.id AS hc_puesto,
                              e.clave AS hc_puesto_clave,
                              GETIDASESOR('$usr',3) as id
                          FROM
                              dep_asesores x
                                  LEFT JOIN
                              asesores_plazas a ON a.id = x.vacante
                                  LEFT JOIN
                              hc_codigos_Departamento b ON a.hc_dep = b.id
                                  LEFT JOIN
                              hc_codigos_Areas c ON b.area = c.id
                                  LEFT JOIN
                              hc_codigos_UnidadDeNegocio d ON c.unidadDeNegocio = d.id
                                  LEFT JOIN
                              hc_codigos_Puesto e ON a.hc_puesto = e.id
                          WHERE
                              x.asesor = GETIDASESOR('$usr',3) AND x.Fecha=CURDATE()") ){

      if($q->num_rows() == 0){

        $idQ = $this->db->query("SELECT GETIDASESOR('$usr',3) as id");
        $idOK = $idQ->row_array();

        return array(
          'vacante'         => null,
          'hc_udn_clave'    => null,
          'hc_udn'          => null,
          'hc_area'         => null,
          'hc_area_clave'   => null,
          'hc_dep_clave'    => null,
          'hc_dep'          => null,
          'hc_puesto'       => null,
          'hc_puesto_clave' => null,
          'id'              => $idOK['id'],
        );
      }
      return $q->row_array();
    }else{
      errResponse('Error de Codigos de Puesto', REST_Controller::HTTP_BAD_REQUEST, $this, 'error', $this->db->error(), 'msg', 'Error al consultar los codigos de puesto del usuario');
    }


  }

  private function tokenAssign( $usr, $preserve ){

    if( $preserve ){
        $expiration = date('Y-m-d H:i:s',strtotime('+ 15 days'));
    }else{
        $expiration = date('Y-m-d H:i:s',strtotime('+ 4 hours'));
    }

    $token = generateToken( array( 'usn' => $usr, 'expire' => $expiration ), 'cAlbertyCome' );

    return array( 'token' => $token, 'expire' => $expiration );
  }



}
