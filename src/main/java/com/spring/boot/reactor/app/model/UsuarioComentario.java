package com.spring.boot.reactor.app.model;

public class UsuarioComentario {

    private Usuario usuario;
    private Comentario comentario;

    public UsuarioComentario(Usuario usuario, Comentario comentario) {
        this.usuario = usuario;
        this.comentario = comentario;
    }

    @Override
    public String toString() {
        return "UsuarioComentario{" +
                "usuario=" + usuario +
                ", comentario=" + comentario +
                '}';
    }
}
