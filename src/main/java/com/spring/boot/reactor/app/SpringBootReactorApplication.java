package com.spring.boot.reactor.app;

import com.spring.boot.reactor.app.model.Comentario;
import com.spring.boot.reactor.app.model.Usuario;
import com.spring.boot.reactor.app.model.UsuarioComentario;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

@SpringBootApplication
public class SpringBootReactorApplication implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(SpringBootReactorApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(SpringBootReactorApplication.class, args);
	}

	public Usuario crearUsuario(){
		return new Usuario("jhon","Doe");
	}

	@Override
	public void run(String... args) throws Exception {
		Contrapresion_V2();
	}

	private void InitialFlux(){
		Flux<String> name = Flux.just("Fernando","Carlos","Juan")
				.doOnNext(e -> System.out.println(e));

		name.subscribe(e->log.info(e));
	}
	private void ErrorFlux(){
		Flux<String> name = Flux.just("Fernando","","Juan")
				.doOnNext(e -> {
					if(e.isEmpty()){
						throw new RuntimeException("No puede ser vacio");
					}else{
						System.out.println(e);
					}
				});

		name.subscribe(e->log.info(e), error -> log.error(error.getMessage()));
	}

	private void Runnable(){
		Flux<String> name = Flux.just("Fernando","Carlos","Juan")
				.doOnNext(e -> {
					if(e.isEmpty()){
						throw new RuntimeException("No puede ser vacio");
					}else{
						System.out.println(e);
					}
				});

		name.subscribe(e->log.info(e), error -> log.error(error.getMessage()), new Runnable() {
			@Override
			public void run() {
				log.info("Ha finalizado la ejecucion con exito!");
			}
		});
	}

	private void FluxObjectMap(){
		Flux<Usuario> name = Flux.just("Fernando","Carlos","Juan")
				.map(nombres -> new Usuario(nombres.toUpperCase(),null))
				.doOnNext(usuario -> {
					if(usuario == null){
						throw new RuntimeException("No puede ser vacio");
					}else{
						System.out.println(usuario.getNombre());
					}
				})
				.map(usuario -> {
					String nombre = usuario.getNombre().toLowerCase();
					usuario.setNombre(nombre);
					return usuario;
				});

		name.subscribe(e->log.info(e.getNombre()), error -> log.error(error.getMessage()), new Runnable() {
			@Override
			public void run() {
				log.info("Ha finalizado la ejecucion con exito!");
			}
		});
	}

	private void FluxFilter(){
		Flux<Usuario> name = Flux.just("Fernando Gordillo","Carlos Rojas","Juan Perez","Juan Palao")
				.map(nombres -> new Usuario(nombres.split(" ")[0].toUpperCase(),nombres.split(" ")[1].toUpperCase()))
				.filter(user -> user.getNombre().equalsIgnoreCase("juan"))
				.doOnNext(usuario -> {
					if(usuario == null){
						throw new RuntimeException("No puede ser vacio");
					}else{
						System.out.println(usuario.getNombre());
					}
				})
				.map(usuario -> {
					String nombre = usuario.getNombre().toLowerCase();
					usuario.setNombre(nombre);
					return usuario;
				});

		name.subscribe(e->log.info(e.toString()), error -> log.error(error.getMessage()), new Runnable() {
			@Override
			public void run() {
				log.info("Ha finalizado la ejecucion con exito!");
			}
		});
	}

	private void FluxInmutable(){
		Flux<String> name = Flux.just("Fernando Gordillo","Carlos Rojas","Juan Perez","Juan Palao");

		Flux<Usuario> usuarios = name.map(nombres -> new Usuario(nombres.split(" ")[0].toUpperCase(),nombres.split(" ")[1].toUpperCase()))
				.filter(user -> user.getNombre().equalsIgnoreCase("juan"))
				.doOnNext(usuario -> {
					if(usuario == null){
						throw new RuntimeException("No puede ser vacio");
					}else{
						System.out.println(usuario.getNombre());
					}
				})
				.map(usuario -> {
					String nombre = usuario.getNombre().toLowerCase();
					usuario.setNombre(nombre);
					return usuario;
				});

		usuarios.subscribe(e->log.info(e.toString()), error -> log.error(error.getMessage()), new Runnable() {
			@Override
			public void run() {
				log.info("Ha finalizado la ejecucion con exito!");
			}
		});
	}

	private void FluxIterable(){
		List<String> usuariosList = new ArrayList<>();
		usuariosList.add("Fernando Gordillo");
		usuariosList.add("Carlos Rojas");
		usuariosList.add("Juan Perez");
		usuariosList.add("Juan Palao");

		Flux<String> name = Flux.fromIterable(usuariosList);

		Flux<Usuario> usuarios = name.map(nombres -> new Usuario(nombres.split(" ")[0].toUpperCase(),nombres.split(" ")[1].toUpperCase()))
				.filter(user -> user.getNombre().equalsIgnoreCase("juan"))
				.doOnNext(usuario -> {
					if(usuario == null){
						throw new RuntimeException("No puede ser vacio");
					}else{
						System.out.println(usuario.getNombre());
					}
				})
				.map(usuario -> {
					String nombre = usuario.getNombre().toLowerCase();
					usuario.setNombre(nombre);
					return usuario;
				});

		usuarios.subscribe(e->log.info(e.toString()), error -> log.error(error.getMessage()), new Runnable() {
			@Override
			public void run() {
				log.info("Ha finalizado la ejecucion con exito!");
			}
		});
	}

	private void FlatMap(){
		List<String> usuariosList = new ArrayList<>();
		usuariosList.add("Fernando Gordillo");
		usuariosList.add("Carlos Rojas");
		usuariosList.add("Juan Perez");
		usuariosList.add("Juan Palao");

		Flux.fromIterable(usuariosList)
				.map(nombres -> new Usuario(nombres.split(" ")[0].toUpperCase(),nombres.split(" ")[1].toUpperCase()))
				.flatMap(user -> {
					if(user.getNombre().equalsIgnoreCase("Juan")){
						return Mono.just(user);
					}else{
						return Mono.empty();
					}
				})
				.map(usuario -> {
					String nombre = usuario.getNombre().toLowerCase();
					usuario.setNombre(nombre);
					return usuario;
				}).subscribe(u->log.info(u.toString()));
	}

	private void ConverFluxListToFluxString(){
		List<Usuario> usuariosList = new ArrayList<>();
		usuariosList.add(new Usuario("Fernando", "Gordillo"));
		usuariosList.add(new Usuario("Carlos", "Rojas"));
		usuariosList.add(new Usuario("Juan", "Perez"));
		usuariosList.add(new Usuario("Juan", "Palao"));

		Flux.fromIterable(usuariosList)
				.map(usuario -> usuario.getNombre().toUpperCase().concat(" ").concat(usuario.getApellido().toUpperCase()))
				.flatMap(user -> {
					if(user.contains("Juan".toUpperCase())){
						return Mono.just(user);
					}else{
						return Mono.empty();
					}
				})
				.map(usuario -> {
					return usuario.toLowerCase();
				}).subscribe(u->log.info(u.toString()));
	}

	private void ConvertFluxToMono(){
		List<Usuario> usuariosList = new ArrayList<>();
		usuariosList.add(new Usuario("Fernando", "Gordillo"));
		usuariosList.add(new Usuario("Carlos", "Rojas"));
		usuariosList.add(new Usuario("Juan", "Perez"));
		usuariosList.add(new Usuario("Juan", "Palao"));

		Flux.fromIterable(usuariosList)
				.collectList()
				.subscribe(lista -> {
					lista.forEach(item -> log.info(item.toString()));
				});
	}

	private void combinarDosFlujosFlatMap(){
		Mono<Usuario> usuarioMono = Mono.fromCallable(() -> crearUsuario());

		Mono<Comentario> comentariosUsuarioMono = Mono.fromCallable(() ->{
			Comentario comentario = new Comentario();
			comentario.addComentario("Hola que tal");
			comentario.addComentario("como estas?");
			return comentario;
		});

		usuarioMono.flatMap(usuario -> comentariosUsuarioMono.map(comentario -> new UsuarioComentario(usuario,comentario)))
				.subscribe(usuarioComentario -> log.info(usuarioComentario.toString()));
	}

	private void combinarDosFlujosZipWith_V1(){
		Mono<Usuario> usuarioMono = Mono.fromCallable(() -> new Usuario("jhon","Doe"));

		Mono<Comentario> comentariosUsuarioMono = Mono.fromCallable(() ->{
			Comentario comentario = new Comentario();
			comentario.addComentario("Hola que tal");
			comentario.addComentario("como estas?");
			return comentario;
		});

		Mono<UsuarioComentario> usuarioConComentarios =	usuarioMono
				.zipWith(comentariosUsuarioMono,(usuario,comentariosUsuario) -> new UsuarioComentario(usuario,comentariosUsuario));
		usuarioConComentarios.subscribe(usuarioComentario -> log.info(usuarioComentario.toString()));
	}

	private void combinarDosFlijosZipWith_V2(){
		Mono<Usuario> usuarioMono = Mono.fromCallable(() -> new Usuario("jhon","Doe"));

		Mono<Comentario> comentariosUsuarioMono = Mono.fromCallable(() ->{
			Comentario comentario = new Comentario();
			comentario.addComentario("Hola que tal");
			comentario.addComentario("como estas?");
			return comentario;
		});

		Mono<UsuarioComentario> usuarioConComentarios =	usuarioMono
				.zipWith(comentariosUsuarioMono)
				.map(tuple -> {
					Usuario u = tuple.getT1();
					Comentario c = tuple.getT2();
					return new UsuarioComentario(u,c);
				});
		usuarioConComentarios.subscribe(usuarioComentario -> log.info(usuarioComentario.toString()));
	}

	private void ZipWithRangos(){
		Flux<Integer> rangos=Flux.range(0,4);
		Flux.just(1,2,3,4)
				.map(i-> i*2)
				.zipWith(rangos,(uno,dos)-> String.format("Primer Flux:%d, Segundo Flux: %d",uno,dos))
				.subscribe(texto ->log.info(texto));
	}

	private void IntervalZipWith(){
		Flux<Integer> rango = Flux.range(1,12);
		Flux<Long> retraso = Flux.interval(Duration.ofSeconds(1));

	/*rango.zipWith(retraso,(ra,re) -> ra )
			.subscribe(i->log.info(i.toString()));*/


		rango.zipWith(retraso,(ra,re) -> ra )
				.doOnNext(i->log.info(i.toString()))
				.blockLast();
	}

	private void IntervaloDelayElements(){
		Flux<Integer> rango = Flux.range(1,12)
				.delayElements(Duration.ofSeconds(1))
				.doOnNext(i->log.info(i.toString()));

		//rango.blockLast();
		rango.subscribe();
	}

	private void IntervaloInfinitoAndRetry() throws InterruptedException {
		CountDownLatch countDownLatch = new CountDownLatch(1);

		Flux.interval(Duration.ofSeconds(1))
				.doOnTerminate(()->countDownLatch.countDown())
				.flatMap(i ->{
					if(i>=5){
						return Flux.error(new InterruptedException("Solo hasta 5!"));
					}
					return Flux.just(i);

				})
				.map(i-> "Hola " + i)
				.retry(2)
				.subscribe(s->log.info(s),e-> log.error(e.getMessage()));

		countDownLatch.await();
	}

	private void FluxCreate(){

		Flux.create(emitter -> {
			Timer time = new Timer();
			time.schedule(new TimerTask() {

				private Integer contador = 0;
				@Override
				public void run() {
					emitter.next(++contador);
					if(contador==10){
						time.cancel();
						emitter.complete();
					}
					if(contador==5){
						time.cancel();
						emitter.error(new InterruptedException("Error, se ha detenido el flux en 5"));
					}
				}
			}, 1000, 1000);
		})
				.subscribe(next -> log.info(next.toString()),
						error -> log.error(error.getMessage()),
						() -> log.info("Hemos terminado"));
	}

	private void Contrapresion_V1(){
		Flux.range(1,10)
				.log()
				.subscribe(new Subscriber<Integer>() {

					private Subscription s;
					private Integer limite = 2;
					private Integer consumido = 0;
					@Override
					public void onSubscribe(Subscription s) {
						this.s=s;
						s.request(limite);

					}

					@Override
					public void onNext(Integer integer) {
						log.info(integer.toString());
						consumido++;
						if(consumido==limite){
							consumido=0;
							s.request(limite);
						}
					}

					@Override
					public void onError(Throwable throwable) {

					}

					@Override
					public void onComplete() {

					}
				});
	}

	private void Contrapresion_V2(){
		Flux.range(1,10)
				.log()
				.limitRate(2)
				.subscribe();
	}
}
