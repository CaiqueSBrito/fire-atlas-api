package com.fireatlas;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RestController;

import static com.tngtech.archunit.core.domain.JavaCall.Predicates.target;
import static com.tngtech.archunit.core.domain.properties.HasName.Predicates.nameMatching;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.Architectures.layeredArchitecture;

/**
 * Estrutura do projeto: pastas por ENTIDADE (detection, sensor, calendar, ...) e, dentro de cada uma, as CAMADAS
 * (controller, dto, entity, mapper, policy, repository, service, integration, cli).
 * As dependencias entre camadas vao em um so sentido: controller/cli -> service -> repository/integration -> entity.
 */
@AnalyzeClasses(packages = "com.fireatlas", importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

    @ArchTest
    static final ArchRule dependencias_entre_camadas = layeredArchitecture()
            .consideringOnlyDependenciesInLayers()
            .layer("Controller").definedBy("com.fireatlas..controller..")     // apresentacao REST
            .layer("Cli").definedBy("com.fireatlas..cli..")                    // apresentacao por linha de comando
            .layer("Service").definedBy("com.fireatlas..service..")            // casos de uso
            .layer("Mapper").definedBy("com.fireatlas..mapper..")              // entidade <-> DTO
            .layer("Dto").definedBy("com.fireatlas..dto..")                    // contratos de resposta
            .layer("Repository").definedBy("com.fireatlas..repository..")      // persistencia
            .layer("Integration").definedBy("com.fireatlas..integration..")    // fonte externa (data lake)
            .layer("Domain").definedBy("com.fireatlas..entity..", "com.fireatlas..policy..") // entidades e regras puras
            .whereLayer("Controller").mayNotBeAccessedByAnyLayer()
            .whereLayer("Cli").mayNotBeAccessedByAnyLayer()
            .whereLayer("Service").mayOnlyBeAccessedByLayers("Controller", "Cli")
            .whereLayer("Mapper").mayOnlyBeAccessedByLayers("Service")
            .whereLayer("Dto").mayOnlyBeAccessedByLayers("Controller", "Service", "Mapper")
            .whereLayer("Repository").mayOnlyBeAccessedByLayers("Service", "Cli")
            .whereLayer("Integration").mayOnlyBeAccessedByLayers("Service", "Repository", "Cli")
            .whereLayer("Domain").mayOnlyBeAccessedByLayers(
                    "Controller", "Cli", "Service", "Mapper", "Dto", "Repository", "Integration");

    /** Todo arquivo fica dentro de uma camada de uma entidade (nada solto na raiz da entidade). */
    @ArchTest
    static final ArchRule todo_arquivo_esta_em_uma_camada = classes()
            .that().resideInAPackage("com.fireatlas..")
            .and().resideOutsideOfPackages("com.fireatlas", "com.fireatlas.config..")
            .should().resideInAnyPackage("..controller..", "..dto..", "..entity..", "..policy..", "..mapper..",
                    "..repository..", "..service..", "..integration..", "..cli..");

    @ArchTest
    static final ArchRule controllers_na_camada_controller = classes()
            .that().areAnnotatedWith(RestController.class)
            .should().resideInAPackage("com.fireatlas..controller..");

    @ArchTest
    static final ArchRule servicos_na_camada_service = classes()
            .that().areAnnotatedWith(Service.class)
            .should().resideInAPackage("com.fireatlas..service..");

    @ArchTest
    static final ArchRule repositorios_na_camada_repository = classes()
            .that().areAnnotatedWith(Repository.class)
            .should().resideInAPackage("com.fireatlas..repository..");

    @ArchTest
    static final ArchRule mappers_na_camada_mapper = classes()
            .that().haveSimpleNameEndingWith("Mapper")
            .and().resideOutsideOfPackage("com.fireatlas..repository..") // RowMapper de JDBC fica no repositorio
            .should().resideInAPackage("com.fireatlas..mapper..");

    @ArchTest
    static final ArchRule sql_so_na_persistencia = noClasses()
            .that().resideOutsideOfPackages("com.fireatlas..repository..", "com.fireatlas..integration..")
            .should().dependOnClassesThat().resideInAnyPackage("java.sql..", "org.springframework.jdbc..",
                    "org.postgresql..");

    @ArchTest
    static final ArchRule servico_nao_conhece_a_nuvem = noClasses()
            .that().resideInAPackage("com.fireatlas..service..")
            .should().dependOnClassesThat().resideInAnyPackage("software.amazon..");

    /** O H3 vem pronto do S3: o backend so consome. Nenhum codigo converte coordenadas em H3 nem H3 em coordenadas. */
    @ArchTest
    static final ArchRule nao_converte_coordenadas_e_h3 = noClasses()
            .should().callMethodWhere(target(nameMatching(
                    "latLngToCell|latLngToCellAddress|cellToLatLng|cellToBoundary|polygonToCells"
                            + "|polygonToCellsExperimental")));

    @ArchTest
    static final ArchRule dominio_sem_framework_web_ou_banco = noClasses()
            .that().resideInAnyPackage("com.fireatlas..entity..", "com.fireatlas..policy..")
            .should().dependOnClassesThat().resideInAnyPackage("org.springframework..", "java.sql..",
                    "jakarta.servlet..");
}
