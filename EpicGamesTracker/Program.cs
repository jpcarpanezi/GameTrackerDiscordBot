using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using EpicGamesTracker.Model;

namespace EpicGamesDataMining {
	class Program {
		private static readonly string _errorsFile = "errors.log";
		private static readonly string _queryFile = "_query.sql";

		static async Task Main(string[] args) {
			try {
				using FileStream openStream = File.OpenRead(@"C:\Users\JP\source\repos\EpicGamesDataMining\EpicGamesDataMining\countries.json");
				List<Countries> countries = await JsonSerializer.DeserializeAsync<List<Countries>>(openStream);
				countries.Sort((x, y) => x.Code.CompareTo(y.Code));

				foreach (Countries country in countries) {
					Console.WriteLine($"[{DateTime.UtcNow}] Processando: {country.Code}");

					HttpClient client = new HttpClient();
					HttpRequestMessage request = new HttpRequestMessage {
						Method = HttpMethod.Post,
						RequestUri = new Uri("https://graphql.epicgames.com/graphql"),
						Content = new StringContent("{\n\t\"query\": \"query searchStoreQuery($allowCountries: String, $category: String, $count: Int, $country: String!, $keywords: String, $locale: String, $namespace: String, $withMapping: Boolean = false, $itemNs: String, $sortBy: String, $sortDir: String, $start: Int, $tag: String, $releaseDate: String, $withPrice: Boolean = false, $withPromotions: Boolean = false, $priceRange: String, $freeGame: Boolean, $onSale: Boolean, $effectiveDate: String) {\\n  Catalog {\\n    searchStore(\\n      allowCountries: $allowCountries\\n      category: $category\\n      count: $count\\n      country: $country\\n      keywords: $keywords\\n      locale: $locale\\n      namespace: $namespace\\n      itemNs: $itemNs\\n      sortBy: $sortBy\\n      sortDir: $sortDir\\n      releaseDate: $releaseDate\\n      start: $start\\n      tag: $tag\\n      priceRange: $priceRange\\n      freeGame: $freeGame\\n      onSale: $onSale\\n      effectiveDate: $effectiveDate\\n    ) {\\n      elements {\\n        title\\n        id\\n        namespace\\n        description\\n        effectiveDate\\n        keyImages {\\n          type\\n          url\\n        }\\n        currentPrice\\n        seller {\\n          id\\n          name\\n        }\\n        productSlug\\n        urlSlug\\n        url\\n        tags {\\n          id\\n        }\\n        items {\\n          id\\n          namespace\\n        }\\n        customAttributes {\\n          key\\n          value\\n        }\\n        categories {\\n          path\\n        }\\n        catalogNs @include(if: $withMapping) {\\n          mappings(pageType: \\\"productHome\\\") {\\n            pageSlug\\n            pageType\\n          }\\n        }\\n        offerMappings @include(if: $withMapping) {\\n          pageSlug\\n          pageType\\n        }\\n        price(country: $country) @include(if: $withPrice) {\\n          totalPrice {\\n            discountPrice\\n            originalPrice\\n            voucherDiscount\\n            discount\\n            currencyCode\\n            currencyInfo {\\n              decimals\\n            }\\n            fmtPrice(locale: $locale) {\\n              originalPrice\\n              discountPrice\\n              intermediatePrice\\n            }\\n          }\\n          lineOffers {\\n            appliedRules {\\n              id\\n              endDate\\n              discountSetting {\\n                discountType\\n              }\\n            }\\n          }\\n        }\\n        promotions(category: $category) @include(if: $withPromotions) {\\n          promotionalOffers {\\n            promotionalOffers {\\n              startDate\\n              endDate\\n              discountSetting {\\n                discountType\\n                discountPercentage\\n              }\\n            }\\n          }\\n          upcomingPromotionalOffers {\\n            promotionalOffers {\\n              startDate\\n              endDate\\n              discountSetting {\\n                discountType\\n                discountPercentage\\n              }\\n            }\\n          }\\n        }\\n      }\\n      paging {\\n        count\\n        total\\n      }\\n    }\\n  }\\n}\\n\",\n\t\"variables\": {\n\t\t\"category\": \"games/edition/base\",\n\t\t\"count\": 40,\n\t\t\"country\": \"" + country.Code.ToString() + "\",\n\t\t\"keywords\": \"\",\n\t\t\"locale\": \"en-US\",\n\t\t\"sortBy\": \"title\",\n\t\t\"sortDir\": \"ASC\",\n\t\t\"allowCountries\": \"" + country.Code.ToString() + "\",\n\t\t\"start\": 0,\n\t\t\"tag\": \"\",\n\t\t\"withMapping\": false,\n\t\t\"withPrice\": true\n\t}\n}") {
							Headers = {
								ContentType = new MediaTypeHeaderValue("application/json")
							}
						}
					};

					using HttpResponseMessage response = await client.SendAsync(request);
					response.EnsureSuccessStatusCode();
					string body = await response.Content.ReadAsStringAsync();
					JObject catalogParse = JObject.Parse(body);

					if (catalogParse["errors"] != null) {
						continue;
					}

					int pagingCount = Convert.ToInt32(catalogParse["data"]["Catalog"]["searchStore"]["paging"]["count"]);
					int pagingTotal = Convert.ToInt32(catalogParse["data"]["Catalog"]["searchStore"]["paging"]["total"]);
					int startTotal = Convert.ToInt32(Math.Round((double)pagingTotal / pagingCount));

					await GetCatalogueFromCountry(country.Code, startTotal);
				}
			} catch (Exception e) {
				await File.AppendAllTextAsync(_errorsFile, $"[{DateTime.UtcNow}] {e + Environment.NewLine}");
			}
		}

		private static async Task GetCatalogueFromCountry(string countryCode, Int32 pagingTotal) {
			for (int i = 0; i <= pagingTotal; i++) {
				try {
					HttpClient client = new HttpClient();
					HttpRequestMessage request = new HttpRequestMessage {
						Method = HttpMethod.Post,
						RequestUri = new Uri("https://graphql.epicgames.com/graphql"),
						Content = new StringContent("{\n\t\"query\": \"query searchStoreQuery($allowCountries: String, $category: String, $count: Int, $country: String!, $keywords: String, $locale: String, $namespace: String, $withMapping: Boolean = false, $itemNs: String, $sortBy: String, $sortDir: String, $start: Int, $tag: String, $releaseDate: String, $withPrice: Boolean = false, $withPromotions: Boolean = false, $priceRange: String, $freeGame: Boolean, $onSale: Boolean, $effectiveDate: String) {\\n  Catalog {\\n    searchStore(\\n      allowCountries: $allowCountries\\n      category: $category\\n      count: $count\\n      country: $country\\n      keywords: $keywords\\n      locale: $locale\\n      namespace: $namespace\\n      itemNs: $itemNs\\n      sortBy: $sortBy\\n      sortDir: $sortDir\\n      releaseDate: $releaseDate\\n      start: $start\\n      tag: $tag\\n      priceRange: $priceRange\\n      freeGame: $freeGame\\n      onSale: $onSale\\n      effectiveDate: $effectiveDate\\n    ) {\\n      elements {\\n        title\\n        id\\n        namespace\\n        description\\n        effectiveDate\\n        keyImages {\\n          type\\n          url\\n        }\\n        currentPrice\\n        seller {\\n          id\\n          name\\n        }\\n        productSlug\\n        urlSlug\\n        url\\n        tags {\\n          id\\n        }\\n        items {\\n          id\\n          namespace\\n        }\\n        customAttributes {\\n          key\\n          value\\n        }\\n        categories {\\n          path\\n        }\\n        catalogNs @include(if: $withMapping) {\\n          mappings(pageType: \\\"productHome\\\") {\\n            pageSlug\\n            pageType\\n          }\\n        }\\n        offerMappings @include(if: $withMapping) {\\n          pageSlug\\n          pageType\\n        }\\n        price(country: $country) @include(if: $withPrice) {\\n          totalPrice {\\n            discountPrice\\n            originalPrice\\n            voucherDiscount\\n            discount\\n            currencyCode\\n            currencyInfo {\\n              decimals\\n            }\\n            fmtPrice(locale: $locale) {\\n              originalPrice\\n              discountPrice\\n              intermediatePrice\\n            }\\n          }\\n          lineOffers {\\n            appliedRules {\\n              id\\n              endDate\\n              discountSetting {\\n                discountType\\n              }\\n            }\\n          }\\n        }\\n        promotions(category: $category) @include(if: $withPromotions) {\\n          promotionalOffers {\\n            promotionalOffers {\\n              startDate\\n              endDate\\n              discountSetting {\\n                discountType\\n                discountPercentage\\n              }\\n            }\\n          }\\n          upcomingPromotionalOffers {\\n            promotionalOffers {\\n              startDate\\n              endDate\\n              discountSetting {\\n                discountType\\n                discountPercentage\\n              }\\n            }\\n          }\\n        }\\n      }\\n      paging {\\n        count\\n        total\\n      }\\n    }\\n  }\\n}\\n\",\n\t\"variables\": {\n\t\t\"category\": \"games/edition/base\",\n\t\t\"count\": 40,\n\t\t\"country\": \"" + countryCode + "\",\n\t\t\"keywords\": \"\",\n\t\t\"locale\": \"en-US\",\n\t\t\"sortBy\": \"title\",\n\t\t\"sortDir\": \"ASC\",\n\t\t\"allowCountries\": \"" + countryCode + "\",\n\t\t\"start\": " + i * 40 + ",\n\t\t\"tag\": \"\",\n\t\t\"withMapping\": false,\n\t\t\"withPrice\": true\n\t}\n}") {
							Headers = {
								ContentType = new MediaTypeHeaderValue("application/json")
							}
						}
					};

					using HttpResponseMessage response = await client.SendAsync(request);
					response.EnsureSuccessStatusCode();
					string body = await response.Content.ReadAsStringAsync();
					JObject catalogParse = JObject.Parse(body);

					if (catalogParse["errors"] != null) {
						continue;
					}

					await File.AppendAllTextAsync($"queries/{countryCode}_{_queryFile}", $"INSERT INTO games(Id, Namespace, Title, Country, OriginalPrice, Currency, ImageUrl) VALUES {Environment.NewLine}");

					for (int j = 0; j < ((JArray)catalogParse["data"]["Catalog"]["searchStore"]["elements"]).Count; j++) {
						string imageUrl = null;
						string countryISO = countryCode;
						string gameId = catalogParse["data"]["Catalog"]["searchStore"]["elements"][j]["id"].ToString();
						string gameNamespace = catalogParse["data"]["Catalog"]["searchStore"]["elements"][j]["namespace"].ToString();
						string gameTitle = catalogParse["data"]["Catalog"]["searchStore"]["elements"][j]["title"].ToString().Replace("'", "\\'");
						string currency = catalogParse["data"]["Catalog"]["searchStore"]["elements"][j]["price"]["totalPrice"]["currencyCode"].ToString();
						uint gamePrice = Convert.ToUInt32(catalogParse["data"]["Catalog"]["searchStore"]["elements"][j]["price"]["totalPrice"]["originalPrice"]);

						for (int k = 0; k < ((JArray)catalogParse["data"]["Catalog"]["searchStore"]["elements"][j]["keyImages"]).Count; k++) {
							if (catalogParse["data"]["Catalog"]["searchStore"]["elements"][j]["keyImages"][k]["type"].ToString() == "OfferImageWide") {
								imageUrl = catalogParse["data"]["Catalog"]["searchStore"]["elements"][j]["keyImages"][k]["url"].ToString();
							}
						}

						if (j == (((JArray)catalogParse["data"]["Catalog"]["searchStore"]["elements"]).Count - 1)) {
							await File.AppendAllTextAsync($"queries/{countryCode}_{_queryFile}", $"('{gameId}', '{gameNamespace}', '{gameTitle}', '{countryISO}', '{gamePrice}', '{currency}', '{imageUrl}') {Environment.NewLine}");
						} else {
							await File.AppendAllTextAsync($"queries/{countryCode}_{_queryFile}", $"('{gameId}', '{gameNamespace}', '{gameTitle}', '{countryISO}', '{gamePrice}', '{currency}', '{imageUrl}'), {Environment.NewLine}");
						}
					}

					await File.AppendAllTextAsync($"queries/{countryCode}_{_queryFile}", $";{Environment.NewLine}");
				} catch (Exception e) {
					await File.AppendAllTextAsync(_errorsFile, $"[{DateTime.UtcNow}] - {countryCode} - {e + Environment.NewLine}");
				}
			}
		}
	}
}
