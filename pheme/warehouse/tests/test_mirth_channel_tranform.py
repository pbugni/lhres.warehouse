from lxml import etree

from pheme.warehouse.mirth_channel_transform import transformer_factory
from pheme.warehouse.mirth_channel_transform import CommonTransferAgent
from pheme.warehouse.mirth_channel_transform import PHEME_http_receiverTransferAgent


def test_transformer_factory_common():
    fake_channel = etree.fromstring("""<channel>
        <id>9c6d9546-bfba-4445-a6bb-f6e2869aaa42</id>
        <name>anything</name>
        </channel>""")
    tf = transformer_factory(fake_channel, None)
    assert(isinstance(tf, CommonTransferAgent))

def test_transformer_factory_http_receiver():
    fake_channel = etree.fromstring("""<channel>
        <id>9c6d9546-bfba-4445-a6bb-f6e2869aaa42</id>
        <name>PHEME_http_receiver</name>
        </channel>""")
    tf = transformer_factory(fake_channel, None)
    assert(isinstance(tf, PHEME_http_receiverTransferAgent))
