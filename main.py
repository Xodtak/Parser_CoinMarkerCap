import requests
import json
import asyncio
import aiohttp
import random
import time
import os
from bs4 import BeautifulSoup as bs


class CoinMarkerCap_parser:
    result_cryptocurrency: [dict] = []
    claster_cryptocurrency_links: [[int, str]] = []
    require_platforms: [str] = ["BNB Smart Chain (BEP20)", "Ethereum", "Polygon"]

    def __init__(self, cryptocurrency_links: [str], header_of_request: dict):
        self.cryptocurrency_links = cryptocurrency_links
        self.header_of_request = header_of_request

    def start_parsing(self):
        start_time = time.time()
        count = 1

        for index in range(len(self.cryptocurrency_links)):
            number_of_link = index + 1
            link_of_cryptocurrency = self.cryptocurrency_links[index]

            self.claster_cryptocurrency_links.append([number_of_link, link_of_cryptocurrency])

            if (number_of_link % 250 == 0 or link_of_cryptocurrency is None or link_of_cryptocurrency == ""):
                start_claster_time = time.time()

                asyncio.run(self.create_tasks_of_parsing())

                self.claster_cryptocurrency_links = []

                print(f"Claster {count} --- %s seconds ---" % (time.time() - start_claster_time))

                self.save_result_cryptocurrency(count=count)
                count += 1

                time.sleep(25)

        self.save_result_cryptocurrency()

        print("Parsing --- %s seconds ---" % (time.time() - start_time))


    def save_result_cryptocurrency(self, count: int|None = None):
        if os.path.exists('results') is False:
            os.mkdir('results')

        name_of_file = f"results/result_{count}.json"
        if count == None:
            name_of_file = f"resualts/resualt.json"

        json.dump(self.result_cryptocurrency, open(name_of_file, "w", encoding="utf-8"), indent=4, ensure_ascii=False)


    def save_error(url: str, error: str):
        with open("errors.csv", "a", encoding="utf-8") as err:
            err.write(f"{url};{error}\n")


    async def create_tasks_of_parsing(self):
        tasks = []
        async with aiohttp.ClientSession() as session:
            for index in range(len(self.claster_cryptocurrency_links)):
                link_claster = self.claster_cryptocurrency_links[index]
                task = asyncio.create_task(self.fetch_content(url=link_claster[1], session=session, index=link_claster[0]))
                tasks.append(task)
                print(f"Задание {link_claster[0]} заявлено")
            await asyncio.gather(*tasks)


    async def fetch_content(self, url: str, session: object, index: int):
        try:
            await asyncio.sleep(random.randint(1, 30))

            async with session.get(url, headers=self.header_of_request) as response:
                if response.status == 200:
                    request = await response.text()
                    page = bs(request,'lxml')

                    content = page.find('script', id='__NEXT_DATA__')
                    content_page = json.loads(content.text)["props"]["pageProps"]["detailRes"]["detail"]

                    result = CoinMarkerCap_parser.get_information_cryptocurrency(url=url, content_page=content_page, number=index, require_platforms=self.require_platforms)

                    if (result):
                        self.result_cryptocurrency.append(result)
                        print(f"Страница {index} успешно просканирована")
                    elif (result is False):
                        print(f"Страница {index} без нужных контрактов")
                    elif (result is None):
                        print(f"Страница {index} не просканирована")
                        
                else:
                    print(f"Страница {index} не просканирована. Ответ {response.status}")
                    CoinMarkerCap_parser.save_error(url=url, error=f"status {response.status}")

        except Exception as error:
            print(error)
            CoinMarkerCap_parser.save_error(url=url, error=error)


    def get_information_cryptocurrency(url: str, content_page: object, number: int, require_platforms: [str]) -> dict|bool:
        try:
            telegram = ['t.me', 'telegram.me']
            check_platform = False

            platforms_token = []

            for platform in content_page["platforms"]:
                if (platform['contractPlatform'] in require_platforms):
                    platforms_token.append({
                        "contractPlatform": platform['contractPlatform'],
                        "contractAddress": platform['contractAddress']
                    })
                    check_platform = True
            
            if (check_platform):
                chats_telegram = []
                for chat in content_page['urls']['chat']:
                    for word_telegram in telegram:
                        if word_telegram in chat:
                            chats_telegram.append(chat)
                            break
                
                channel_telegram = []
                for channel in content_page['urls']['announcement']:
                    for word_telegram in telegram:
                        if word_telegram in channel:
                            chats_telegram.append(channel)
                            break

                information_cryptocurrency = {
                    "name": content_page['name'],
                    "number": number,
                    "coinmarketcap": url,
                    "token_urls": content_page['urls']['website'],
                    "symbol": content_page['symbol'],
                    "platforms": platforms_token,
                    "chat": chats_telegram,
                    "telegram": channel_telegram,
                    "twitter": content_page['urls']['twitter']
                }
                
                return information_cryptocurrency
            else:
                return False
        except Exception as error:
            CoinMarkerCap_parser.save_error(url=url, error=error)
            return None


def parse_cryptocurrency_links() -> [str]:
    req = requests.get('https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing?start=1&limit=9114&sortBy=market_cap&sortType=desc&convert=USD,BTC,ETH&cryptoType=all&tagType=all&audited=false&aux=ath,atl,high24h,low24h,num_market_pairs,cmc_rank,date_added,max_supply,circulating_supply,total_supply,volume_7d,volume_30d,self_reported_circulating_supply,self_reported_market_cap')
    data = req.json()

    basis_link = 'https://coinmarketcap.com/ru/currencies/'
    currency_links = []

    for currency in data['data']['cryptoCurrencyList']:
        currency_links.append(f'{basis_link}{currency["slug"]}/')
    return currency_links


if __name__ == "__main__":
    # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) # For Windows

    header_of_request = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "ru,en-US;q=0.9,en;q=0.8,ru-RU;q=0.7",
        "cache-control": "no-cache",
        "dnt": "1",
        "pragma": "no-cache",
        "sec-ch-ua": '"Google Chrome";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
    }

    cryptocurrency_links = parse_cryptocurrency_links()
    coinMarketCup_parser = CoinMarkerCap_parser(cryptocurrency_links=cryptocurrency_links, header_of_request=header_of_request)
    coinMarketCup_parser.start_parsing()