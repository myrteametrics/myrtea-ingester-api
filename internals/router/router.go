package router

import (
	"time"

	jwt "github.com/dgrijalva/jwt-go"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/cors"
	"github.com/go-chi/jwtauth"
	_ "github.com/myrteametrics/myrtea-ingester-api/v4/docs" // docs is generated by Swag CL
	"github.com/myrteametrics/myrtea-ingester-api/v4/internals/handlers"
	"github.com/myrteametrics/myrtea-sdk/v4/postgres"
	"github.com/myrteametrics/myrtea-sdk/v4/security"
	httpSwagger "github.com/swaggo/http-swagger"
	"go.uber.org/zap"
)

// NewChiRouter initialize a chi.Mux router with all required default middleware (logger, security, recovery, etc.)
func NewChiRouter(apiEnableSecurity bool, apiEnableCORS bool, apiEnableGatewayMode bool, logLevel zap.AtomicLevel) *chi.Mux {
	r := chi.NewRouter()

	// Specific security middleware initialization
	signingKey := []byte(security.RandString(128))
	securityMiddleware := security.NewMiddlewareJWT(signingKey, security.NewDatabaseAuth(postgres.DB()))

	// Global middleware stack
	// TODO: Add CORS middleware
	if apiEnableCORS {
		cors := cors.New(cors.Options{
			AllowedOrigins:   []string{"*"},
			AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
			AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
			ExposedHeaders:   []string{"Link"},
			AllowCredentials: true,
			MaxAge:           300, // Maximum value not ignored by any of major browsers
		})
		r.Use(cors.Handler)
	}

	r.Use(middleware.SetHeader("Strict-Transport-Security", "max-age=63072000; includeSubDomains"))
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.StripSlashes)
	r.Use(middleware.RedirectSlashes)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))

	r.Route("/api/v1", func(r chi.Router) {

		// Public routes
		r.Group(func(r chi.Router) {
			r.Get("/isalive", handlers.IsAlive)
			r.Post("/login", securityMiddleware.GetToken())

			// Temporary Public Swagger
			r.Get("/swagger/*", httpSwagger.WrapHandler)
		})

		// Protected routes
		r.Group(func(rg chi.Router) {
			// if apiEnableSecurity {
			// 	rg.Use(jwtauth.Verifier(jwtauth.New(jwt.SigningMethodHS256.Name, signingKey, nil)))
			// 	rg.Use(jwtauth.Authenticator)
			// }
			if apiEnableSecurity {
				if apiEnableGatewayMode {
					// Warning: No signature verification will be done on JWT.
					// JWT MUST have been verified before by the API Gateway
					rg.Use(UnverifiedAuthenticator)
				} else {
					rg.Use(jwtauth.Verifier(jwtauth.New(jwt.SigningMethodHS256.Name, signingKey, nil)))
					rg.Use(jwtauth.Authenticator)
				}
				// rg.Use(ContextMiddleware)
			}
			rg.Use(middleware.SetHeader("Content-Type", "application/json"))

			rg.HandleFunc("/log_level", logLevel.ServeHTTP)
			rg.Mount("/ingester", ingesterRouter())
		})
	})

	return r
}
