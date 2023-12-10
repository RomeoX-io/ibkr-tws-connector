from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from threading import Thread
import queue
import time
import traceback


# Import AssetManager from db.py
from db import AssetManager
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', filename='app.log', filemode='w')
logger = logging.getLogger(__name__)

class TestApp(EClient, EWrapper):
    def __init__(self, asset_manager):
        EClient.__init__(self, self)
        self.order_id_queue = queue.Queue()
        self.asset_manager = asset_manager
        self.open_orders = {}
    

    def nextValidId(self, orderId):
        super().nextValidId(orderId)
        self.order_id_queue.put(orderId)

    def error(self, reqId, errorCode, errorString):
        logger.error(f"Error. Id: {reqId}, Code: {errorCode}, Msg: {errorString}")

    def openOrder(self, orderId, contract, order, orderState):
        self.open_orders[orderId] = (contract.symbol, order.totalQuantity)

    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        if status in ['Cancelled', 'Filled'] and orderId in self.open_orders:
            del self.open_orders[orderId]

    def cancel_all_orders_for_asset(self, asset):
        logger.info(f"Cancelling all orders for asset: {asset}")
        orders_to_cancel = [orderId for orderId, (symbol, _) in self.open_orders.items() if symbol == asset]
        for orderId in orders_to_cancel:
            self.cancelOrder(orderId)
            logger.info(f"Cancelled order ID: {orderId}")

    def cancel_all_orders_for_asset(self, asset):
        try:
            logger.info(f"Cancelling all orders for asset: {asset}")
            orders_to_cancel = [orderId for orderId, (symbol, _) in self.open_orders.items() if symbol == asset]
            for orderId in orders_to_cancel:
                self.cancelOrder(orderId)
                logger.info(f"Cancelled order ID: {orderId}")
        except Exception as e:
            logger.error(f"Error in cancel_all_orders_for_asset: {e}")

    def check_and_update_orders(self):
        try:
            logger.info("Checking and updating orders...")
            while True:
                assets_to_update = self.asset_manager.get_assets_to_update()

                for asset, amount in assets_to_update.items():
                    self.cancel_all_orders_for_asset(asset)
                    time.sleep(1)  # Short delay to ensure orders are canceled

                    # Update open quantity in the asset manager
                    open_qty = sum(qty for symbol, qty in self.open_orders.values() if symbol == asset)
                    self.asset_manager.update_open(asset, open_qty)

                    # Calculate the amount needed to reach the target
                    amount_needed = self.asset_manager.get_target_amount(asset) - open_qty

                    # Place new order if amount_needed is not zero
                    if amount_needed != 0:
                        self.place_order_for_asset(asset, amount_needed)

                time.sleep(5)

        except Exception as e:
            logger.error(f"Error in check_and_update_orders: {e}")
            traceback.print_exc() 

    def place_order_for_asset(self, asset, amount):
        try:
            # Set limit price high for buy orders and low for sell orders
            limit_price = 1_000_000 if amount > 0 else 0.0001

            logger.info(f"Placing limit order for asset: {asset}, Amount: {amount}, Limit Price: {limit_price}")
            next_order_id = self.order_id_queue.get(block=True)

            contract = Contract()
            contract.symbol = asset
            contract.secType = "STK"    
            contract.exchange = "SMART"
            contract.currency = "USD"

            order = Order()
            order.action = "BUY" if amount > 0 else "SELL"
            order.orderType = "LMT"
            order.totalQuantity = abs(amount)
            order.lmtPrice = limit_price
            order.eTradeOnly = False
            order.firmQuoteOnly = False

            self.placeOrder(next_order_id, contract, order)
        except Exception as e:
            logger.error(f"Error in place_order_for_asset: {e}")
            if "No market data" in str(e):
                logger.warning(f"Skipping order for {asset} due to lack of market data.")
            else:
                logger.error(f"Unhandled error for {asset}: {e}")
                
    


    def stop(self):
        self.done = True
        self.disconnect()

    def run(self):
        self.done = False
        while not self.done:
            time.sleep(0.1)



def main():
    try:
        asset_manager = AssetManager('assets.json')

        app = TestApp(asset_manager)
        app.connect("127.0.0.1", 7497, clientId=100)

        thread = Thread(target=app.run)
        thread.start()

        order_thread = Thread(target=app.check_and_update_orders)
        order_thread.start()

        # Wait for the threads to complete their tasks
        thread.join()
        order_thread.join()

    except KeyboardInterrupt:
        print("Interrupted by user, shutting down")
        app.stop()
        thread.join()
        order_thread.join()
        logger.info("Successfully disconnected and shut down.")

if __name__ == "__main__":
    main()
