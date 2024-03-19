import grpc
import trader.qmt.qmt_pb2 as qmt_pb2
import trader.qmt.qmt_pb2_grpc as qmt_pb2_grpc
import time


class qmtClient():
    def __init__(self) -> None:
        self.channel = grpc.insecure_channel('192.168.8.119:50051')
        # 使用该通道创建一个存根
        self.stub = qmt_pb2_grpc.QmtServiceStub(self.channel)

    def protobuf_to_dict(self,obj):
        # Base case for recursion termination
        if not hasattr(obj, 'DESCRIPTOR'):
            return obj

        result = {}
        for field in obj.DESCRIPTOR.fields:
            value = getattr(obj, field.name)
            # Convert repeated message fields to list of dicts
            if field.message_type and field.label == field.LABEL_REPEATED:
                result[field.name] = [self.protobuf_to_dict(v) for v in value]
            # Convert non-repeated message fields to dict
            elif field.message_type:
                result[field.name] = self.protobuf_to_dict(value)
            # Convert repeated scalar fields to list
            elif field.label == field.LABEL_REPEATED:
                result[field.name] = list(value)
            # Convert scalar fields directly
            else:
                result[field.name] = value
        return result

    def assetSync(self,context):
        asset= self.getAsset()
        context['portfolio']['cash']=asset['cash']
        context['portfolio']['transferable_cash']=asset['cash']-asset['frozen_cash']
        context['portfolio']['locked_cash']=asset['frozen_cash']
        context['portfolio']['total_value']=asset['total_value']
        context['portfolio']['positions_value']=asset['market_value']
        context['portfolio']['portfolio_value=0']=asset['market_value']
        return asset

    def positionSync(self,context):
        positions={

        }
        pos_list= self.GetPositions()
        for pos in pos_list:
            positions[pos['stock_code']]={
                "code":pos['stock_code'],
                "amount":pos['volume'],
                "enable_amount":pos['can_use_volume'],
                "last_sale_price":pos['open_price'],
                "cost_basis":0,
                "total_value":pos['market_value'],
            }
        context['portfolio']['positions']=positions

    def sync(self,context):
        print("同步实盘数据")
        self.assetSync(context)
        self.positionSync(context)


    def getPrice(self,code):
        try:
            response=self.stub.GetPrice(qmt_pb2.PriceRequest(code=code))
            asset= self.protobuf_to_dict(response)
            return asset['last_price']
        except grpc._channel._InactiveRpcError as e:
            print(f"RPC failed: {e.code()}")
            print(f"Details: {e.details()}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None


    def getInfo(self,code):
        try:
            response=self.stub.GetDailyInfo(qmt_pb2.DailyInfoRequest(code=code))
            info=self.protobuf_to_dict(response)
            info['downLimit']=info['down_limit']
            info['upLimit']=info['up_limit']
            return info
        except grpc._channel._InactiveRpcError as e:
            print(f"RPC failed: {e.code()}")
            print(f"Details: {e.details()}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None
        
    def OrderBuy(self,code, amount, price=0, strategy='strategy', remark='remark'):
        response=self.stub.OrderBuy(qmt_pb2.OrderRequest(code=code, amount=amount, price=price, strategy=strategy, remark=remark))
        seq=self.protobuf_to_dict(response)
        return seq['seq']

    def OrderSell(self,code, amount, price=0, strategy='strategy', remark='remark'):
        response=self.stub.OrderSell(qmt_pb2.OrderRequest(code=code, amount=amount, price=price, strategy=strategy, remark=remark))
        seq=self.protobuf_to_dict(response)
        return seq

    def QueryOrders(self):
        response=self.stub.QueryOrders(qmt_pb2.QueryOrdersRequest())
        orders=self.protobuf_to_dict(response)
        return orders

    def CancelOrders(self):
        response=self.stub.CancelOrders(qmt_pb2.QueryOrdersRequest())
        return True
    
    def RetryOrders(self):
        response=self.stub.RetryOrders(qmt_pb2.QueryOrdersRequest())
        return True

    def getAsset(self):
        response=self.stub.GetAsset(qmt_pb2.AssetRequest())
        asset=self.protobuf_to_dict(response)
        return asset

    def GetPositions(self):
        response=self.stub.GetPositions(qmt_pb2.AssetRequest())
        positions=self.protobuf_to_dict(response)
        return positions['positions']

qclient=qmtClient()